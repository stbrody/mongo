// d_migrate.cpp

/**
*    Copyright (C) 2008-2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects
*    for all of the code used other than as permitted herein. If you modify
*    file(s) with this exception, you may extend this exception to your
*    version of the file(s), but you are not obligated to do so. If you do not
*    wish to do so, delete this exception statement from your version. If you
*    delete this exception statement from all source files in the program,
*    then also delete it in the license file.
*/


/**
   these are commands that live in mongod
   mostly around shard management and checking
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include <algorithm>
#include <boost/thread/thread.hpp>
#include <map>
#include <string>
#include <vector>

#include "mongo/client/connpool.h"
#include "mongo/client/dbclientcursor.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/lock_state.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/storage/mmap_v1/dur.h"
#include "mongo/db/field_parser.h"
#include "mongo/db/hasher.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/repl_coordinator_global.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/write_concern.h"
#include "mongo/logger/ramlog.h"
#include "mongo/s/chunk.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/config.h"
#include "mongo/s/d_state.h"
#include "mongo/s/distlock.h"
#include "mongo/s/shard.h"
#include "mongo/s/type_chunk.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/elapsed_tracker.h"
#include "mongo/util/exit.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/queue.h"
#include "mongo/util/startup_test.h"

// Pause while a fail point is enabled.
#define MONGO_FP_PAUSE_WHILE(symbol) while (MONGO_FAIL_POINT(symbol)) { sleepmillis(100); }

using namespace std;

namespace {
    using mongo::WriteConcernOptions;
    using mongo::repl::ReplicationCoordinator;

    const int kDefaultWTimeoutMs = 60 * 1000;
    const WriteConcernOptions DefaultWriteConcern(2, WriteConcernOptions::NONE, kDefaultWTimeoutMs);

    /**
     * Returns the default write concern for migration cleanup (at donor shard) and
     * cloning documents (at recipient shard).
     */
    WriteConcernOptions getDefaultWriteConcern() {
        ReplicationCoordinator* replCoordinator =
                mongo::repl::getGlobalReplicationCoordinator();

        if (replCoordinator->getReplicationMode() ==
                mongo::repl::ReplicationCoordinator::modeReplSet) {
            mongo::Status status =
                replCoordinator->checkIfWriteConcernCanBeSatisfied(DefaultWriteConcern);

            if (status.isOK()) {
              return DefaultWriteConcern;
            }
        }

        return WriteConcernOptions(1, WriteConcernOptions::NONE, 0);
    }
}

namespace mongo {

    MONGO_FP_DECLARE(failMigrationCommit);
    MONGO_FP_DECLARE(failMigrationConfigWritePrepare);
    MONGO_FP_DECLARE(failMigrationApplyOps);

    Tee* migrateLog = RamLog::get("migrate");

    class MoveTimingHelper {
    public:
        MoveTimingHelper( const string& where , const string& ns , BSONObj min , BSONObj max ,
                          int total, string* cmdErrmsg, string toShard, string fromShard )
            : _where( where ) , _ns( ns ) , _to( toShard ), _from( fromShard ), _next( 0 ),
            _total( total ) , _cmdErrmsg( cmdErrmsg ) {
            _b.append( "min" , min );
            _b.append( "max" , max );
        }

        ~MoveTimingHelper() {
            // even if logChange doesn't throw, bson does
            // sigh
            try {
                if ( !_to.empty() ){
                    _b.append( "to", _to );
                }
                if ( !_from.empty() ){
                    _b.append( "from", _from );
                }
                if ( _next != _total ) {
                    _b.append( "note" , "aborted" );
                }
                else {
                    _b.append( "note" , "success" );
                }
                if ( !_cmdErrmsg->empty() ) {
                    _b.append( "errmsg" , *_cmdErrmsg );
                }
                configServer.logChange( (string)"moveChunk." + _where , _ns, _b.obj() );
            }
            catch ( const std::exception& e ) {
                warning() << "couldn't record timing for moveChunk '" << _where << "': " << e.what() << migrateLog;
            }
        }

        void done( int step ) {
            verify( step == ++_next );
            verify( step <= _total );

            stringstream ss;
            ss << "step " << step << " of " << _total;
            string s = ss.str();

            CurOp * op = cc().curop();
            if ( op )
                op->setMessage( s.c_str() );
            else
                warning() << "op is null in MoveTimingHelper::done" << migrateLog;

            _b.appendNumber( s , _t.millis() );
            _t.reset();

#if 0
            // debugging for memory leak?
            ProcessInfo pi;
            ss << " v:" << pi.getVirtualMemorySize()
               << " r:" << pi.getResidentSize();
            log() << ss.str() << migrateLog;
#endif
        }

    private:
        Timer _t;

        string _where;
        string _ns;
        string _to;
        string _from;

        int _next;
        int _total; // expected # of steps

        const string* _cmdErrmsg;

        BSONObjBuilder _b;

    };

    class ChunkCommandHelper : public Command {
    public:
        ChunkCommandHelper( const char * name )
            : Command( name ) {
        }

        virtual void help( stringstream& help ) const {
            help << "internal - should not be called directly";
        }
        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

    };

    bool isInRange( const BSONObj& obj ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {
        ShardKeyPattern shardKey( shardKeyPattern );
        BSONObj k = shardKey.extractShardKeyFromDoc( obj );
        return k.woCompare( min ) >= 0 && k.woCompare( max ) < 0;
    }


    class MigrateFromStatus {
    public:
        MigrateFromStatus() : _mutex("MigrateFromStatus") {
            _active = false;
            _inCriticalSection = false;
            _memoryUsed = 0;
        }

        /**
         * @return false if cannot start. One of the reason for not being able to
         *     start is there is already an existing migration in progress.
         */
        bool start( const std::string& ns ,
                    const BSONObj& min ,
                    const BSONObj& max ,
                    const BSONObj& shardKeyPattern ) {

            scoped_lock l(_mutex); // reads and writes _active

            if (_active) {
                return false;
            }

            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );
            verify( ns.size() );

            _ns = ns;
            _min = min;
            _max = max;
            _shardKeyPattern = shardKeyPattern;

            verify( _cloneLocs.size() == 0 );
            verify( _deleted.size() == 0 );
            verify( _reload.size() == 0 );
            verify( _memoryUsed == 0 );

            _active = true;
            return true;
        }

        void done(OperationContext* txn) {
            log() << "MigrateFromStatus::done About to acquire global write lock to exit critical "
                    "section" << endl;

            _deleteNotifyExec.reset( NULL );

            // TODO: Change this. This is a bad hack for protecting some of the data structures
            // below that were not properly synchronized with the intended latches in some
            // usages.
            Lock::DBLock dbLock(txn->lockState(), nsToDatabaseSubstring(_ns), MODE_IX);
            Lock::CollectionLock collLock(txn->lockState(), _ns, MODE_X);
            log() << "MigrateFromStatus::done coll lock for " << _ns << " acquired" << endl;

            {
                scoped_spinlock lk( _trackerLocks );
                _deleted.clear();
                _reload.clear();
                _cloneLocs.clear();
            }
            _memoryUsed = 0;

            scoped_lock l(_mutex);
            _active = false;
            _inCriticalSection = false;
            _inCriticalSectionCV.notify_all();
        }

        void logOp(OperationContext* txn,
                   const char* opstr,
                   const char* ns,
                   const BSONObj& obj,
                   BSONObj* patt,
                   bool notInActiveChunk) {
            if ( ! _getActive() )
                return;

            if ( _ns != ns )
                return;

            // no need to log if this is not an insertion, an update, or an actual deletion
            // note: opstr 'db' isn't a deletion but a mention that a database exists (for replication
            // machinery mostly)
            char op = opstr[0];
            if ( op == 'n' || op =='c' || ( op == 'd' && opstr[1] == 'b' ) )
                return;

            BSONElement ide;
            if ( patt )
                ide = patt->getField( "_id" );
            else
                ide = obj["_id"];

            if ( ide.eoo() ) {
                warning() << "logOpForSharding got mod with no _id, ignoring  obj: " << obj << migrateLog;
                return;
            }

            BSONObj it;

            switch ( opstr[0] ) {

            case 'd': {

                if (notInActiveChunk) {
                    // we don't want to xfer things we're cleaning
                    // as then they'll be deleted on TO
                    // which is bad
                    return;
                }

                // can't filter deletes :(
                _deleted.push_back( ide.wrap() );
                _memoryUsed += ide.size() + 5;
                return;
            }

            case 'i':
                it = obj;
                break;

            case 'u':
                Client::Context ctx(txn,  _ns );
                if ( ! Helpers::findById( txn, ctx.db(), _ns.c_str(), ide.wrap(), it ) ) {
                    warning() << "logOpForSharding couldn't find: " << ide << " even though should have" << migrateLog;
                    return;
                }
                break;

            }

            if ( ! isInRange( it , _min , _max , _shardKeyPattern ) )
                return;

            _reload.push_back( ide.wrap() );
            _memoryUsed += ide.size() + 5;
        }

        void xfer( OperationContext* txn, Database* db, list<BSONObj> * l , BSONObjBuilder& b , const char * name , long long& size , bool explode ) {
            const long long maxSize = 1024 * 1024;

            if ( l->size() == 0 || size > maxSize )
                return;

            BSONArrayBuilder arr(b.subarrayStart(name));

            list<BSONObj>::iterator i = l->begin();

            while ( i != l->end() && size < maxSize ) {
                BSONObj t = *i;
                if ( explode ) {
                    BSONObj it;
                    if ( Helpers::findById( txn, db , _ns.c_str() , t, it ) ) {
                        arr.append( it );
                        size += it.objsize();
                    }
                }
                else {
                    arr.append( t );
                }
                i = l->erase( i );
                size += t.objsize();
            }

            arr.done();
        }

        /**
         * called from the dest of a migrate
         * transfers mods from src to dest
         */
        bool transferMods(OperationContext* txn, string& errmsg, BSONObjBuilder& b) {
            if ( ! _getActive() ) {
                errmsg = "no active migration!";
                return false;
            }

            long long size = 0;

            {
                AutoGetCollectionForRead ctx(txn, _ns);

                xfer(txn, ctx.getDb(), &_deleted, b, "deleted", size, false);
                xfer(txn, ctx.getDb(), &_reload, b, "reload", size, true);
            }

            b.append( "size" , size );

            return true;
        }

        /**
         * Get the disklocs that belong to the chunk migrated and sort them in _cloneLocs (to avoid seeking disk later)
         *
         * @param maxChunkSize number of bytes beyond which a chunk's base data (no indices) is considered too large to move
         * @param errmsg filled with textual description of error if this call return false
         * @return false if approximate chunk size is too big to move or true otherwise
         */
        bool storeCurrentLocs(OperationContext* txn,
                              long long maxChunkSize,
                              string& errmsg,
                              BSONObjBuilder& result ) {
            AutoGetCollectionForRead ctx(txn, _ns);
            Collection* collection = ctx.getCollection();
            if ( !collection ) {
                errmsg = "ns not found, should be impossible";
                return false;
            }

            invariant( _deleteNotifyExec.get() == NULL );
            WorkingSet* ws = new WorkingSet();
            DeleteNotificationStage* dns = new DeleteNotificationStage();
            PlanExecutor* deleteNotifyExec;
            // Takes ownership of 'ws' and 'dns'.
            Status execStatus = PlanExecutor::make(txn,
                                                   ws,
                                                   dns,
                                                   collection,
                                                   PlanExecutor::YIELD_MANUAL,
                                                   &deleteNotifyExec);
            invariant(execStatus.isOK());
            deleteNotifyExec->registerExec();
            _deleteNotifyExec.reset(deleteNotifyExec);

            // Allow multiKey based on the invariant that shard keys must be single-valued.
            // Therefore, any multi-key index prefixed by shard key cannot be multikey over
            // the shard key fields.
            IndexDescriptor *idx =
                collection->getIndexCatalog()->findIndexByPrefix( txn,
                                                                  _shardKeyPattern ,
                                                                  false );  /* allow multi key */

            if ( idx == NULL ) {
                errmsg = (string)"can't find index in storeCurrentLocs" + causedBy( errmsg );
                return false;
            }
            // Assume both min and max non-empty, append MinKey's to make them fit chosen index
            KeyPattern kp( idx->keyPattern() );
            BSONObj min = Helpers::toKeyFormat( kp.extendRangeBound( _min, false ) );
            BSONObj max = Helpers::toKeyFormat( kp.extendRangeBound( _max, false ) );

            auto_ptr<PlanExecutor> exec(
                InternalPlanner::indexScan(txn, collection, idx, min, max, false));
            // We can afford to yield here because any change to the base data that we might
            // miss is already being queued and will migrate in the 'transferMods' stage.
            exec->setYieldPolicy(PlanExecutor::YIELD_AUTO);

            // use the average object size to estimate how many objects a full chunk would carry
            // do that while traversing the chunk's range using the sharding index, below
            // there's a fair amount of slack before we determine a chunk is too large because object sizes will vary
            unsigned long long maxRecsWhenFull;
            long long avgRecSize;
            const long long totalRecs = collection->numRecords(txn);
            if ( totalRecs > 0 ) {
                avgRecSize = collection->dataSize(txn) / totalRecs;
                maxRecsWhenFull = maxChunkSize / avgRecSize;
                maxRecsWhenFull = std::min( (unsigned long long)(Chunk::MaxObjectPerChunk + 1) , 130 * maxRecsWhenFull / 100 /* slack */ );
            }
            else {
                avgRecSize = 0;
                maxRecsWhenFull = Chunk::MaxObjectPerChunk + 1;
            }
            
            // do a full traversal of the chunk and don't stop even if we think it is a large chunk
            // we want the number of records to better report, in that case
            bool isLargeChunk = false;
            unsigned long long recCount = 0;;
            DiskLoc dl;
            while (PlanExecutor::ADVANCED == exec->getNext(NULL, &dl)) {
                if ( ! isLargeChunk ) {
                    scoped_spinlock lk( _trackerLocks );
                    _cloneLocs.insert( dl );
                }

                if ( ++recCount > maxRecsWhenFull ) {
                    isLargeChunk = true;
                }
            }
            exec.reset();

            if ( isLargeChunk ) {
                warning() << "cannot move chunk: the maximum number of documents for a chunk is "
                          << maxRecsWhenFull << " , the maximum chunk size is " << maxChunkSize
                          << " , average document size is " << avgRecSize << ". Found "
                          << recCount << " documents in chunk " << " ns: " << _ns << " " << _min
                          << " -> " << _max << migrateLog;
                result.appendBool( "chunkTooBig" , true );
                result.appendNumber( "estimatedChunkSize" , (long long)(recCount * avgRecSize) );
                errmsg = "chunk too big to move";
                return false;
            }

            {
                scoped_spinlock lk( _trackerLocks );
                log() << "moveChunk number of documents: " << _cloneLocs.size() << migrateLog;
            }
            txn->recoveryUnit()->commitAndRestart();
            return true;
        }

        bool clone(OperationContext* txn, string& errmsg , BSONObjBuilder& result ) {
            if ( ! _getActive() ) {
                errmsg = "not active";
                return false;
            }

            ElapsedTracker tracker (128, 10); // same as ClientCursor::_yieldSometimesTracker

            int allocSize;
            {
                AutoGetCollectionForRead ctx(txn, _ns);
                Collection* collection = ctx.getCollection();
                invariant(collection);
                scoped_spinlock lk( _trackerLocks );
                allocSize =
                    std::min(BSONObjMaxUserSize,
                             (int)((12 + collection->averageObjectSize(txn)) * _cloneLocs.size()));
            }
            BSONArrayBuilder a (allocSize);
            
            while ( 1 ) {
                bool filledBuffer = false;
                
                AutoGetCollectionForRead ctx(txn, _ns);
                Collection* collection = ctx.getCollection();

                scoped_spinlock lk( _trackerLocks );
                set<DiskLoc>::iterator i = _cloneLocs.begin();
                for ( ; i!=_cloneLocs.end(); ++i ) {
                    if (tracker.intervalHasElapsed()) // should I yield?
                        break;
                    
                    invariant( collection );

                    DiskLoc dl = *i;
                    BSONObj o;
                    if ( !collection->findDoc( txn, dl, &o ) ) {
                        // doc was deleted
                        continue;
                    }

                    // use the builder size instead of accumulating 'o's size so that we take into consideration
                    // the overhead of BSONArray indices, and *always* append one doc
                    if ( a.arrSize() != 0 &&
                         a.len() + o.objsize() + 1024 > BSONObjMaxUserSize ) {
                        filledBuffer = true; // break out of outer while loop
                        break;
                    }
                    
                    a.append( o );
                }
                
                _cloneLocs.erase( _cloneLocs.begin() , i );
                
                if ( _cloneLocs.empty() || filledBuffer )
                    break;
            }

            result.appendArray( "objects" , a.arr() );
            return true;
        }

        void aboutToDelete( const DiskLoc& dl ) {
            // Even though above we call findDoc to check for existance
            // that check only works for non-mmapv1 engines, and this is needed
            // for mmapv1.

            // lock not needed right now
            // but trying to prevent a future bug
            scoped_spinlock lk( _trackerLocks );
            _cloneLocs.erase( dl );
        }

        std::size_t cloneLocsRemaining() {
            scoped_spinlock lk( _trackerLocks );
            return _cloneLocs.size();
        }

        long long mbUsed() const { return _memoryUsed / ( 1024 * 1024 ); }

        bool getInCriticalSection() const {
            scoped_lock l(_mutex);
            return _inCriticalSection;
        }

        void setInCriticalSection( bool b ) {
            scoped_lock l(_mutex);
            _inCriticalSection = b;
            _inCriticalSectionCV.notify_all();
        }

        /**
         * @return true if we are NOT in the critical section
         */
        bool waitTillNotInCriticalSection( int maxSecondsToWait ) {
            boost::xtime xt;
            boost::xtime_get(&xt, MONGO_BOOST_TIME_UTC);
            xt.sec += maxSecondsToWait;

            scoped_lock l(_mutex);
            while ( _inCriticalSection ) {
                if ( ! _inCriticalSectionCV.timed_wait( l.boost(), xt ) )
                    return false;
            }

            return true;
        }

        bool isActive() const { return _getActive(); }

    private:
        mutable mongo::mutex _mutex; // protect _inCriticalSection and _active
        boost::condition _inCriticalSectionCV;

        bool _inCriticalSection;
        bool _active;

        string _ns;
        BSONObj _min;
        BSONObj _max;
        BSONObj _shardKeyPattern;

        // we need the lock in case there is a malicious _migrateClone for example
        // even though it shouldn't be needed under normal operation
        SpinLock _trackerLocks;

        // disk locs yet to be transferred from here to the other side
        // no locking needed because built initially by 1 thread in a read lock
        // emptied by 1 thread in a read lock
        // updates applied by 1 thread in a write lock
        set<DiskLoc> _cloneLocs;

        list<BSONObj> _reload; // objects that were modified that must be recloned
        list<BSONObj> _deleted; // objects deleted during clone that should be deleted later
        long long _memoryUsed; // bytes in _reload + _deleted

        bool _getActive() const { scoped_lock l(_mutex); return _active; }
        void _setActive( bool b ) { scoped_lock l(_mutex); _active = b; }

        /**
         * Used to receive invalidation notifications.
         *
         * XXX: move to the exec/ directory.
         */
        class DeleteNotificationStage : public PlanStage {
        public:
            virtual void invalidate(const DiskLoc& dl, InvalidationType type);

            virtual StageState work(WorkingSetID* out) {
                invariant( false );
            }
            virtual bool isEOF() {
                invariant( false );
                return false;
            }
            virtual void kill() {
            }
            virtual void saveState() {
                invariant( false );
            }
            virtual void restoreState(OperationContext* opCtx) {
                invariant( false );
            }
            virtual PlanStageStats* getStats() {
                invariant( false );
                return NULL;
            }
            virtual CommonStats* getCommonStats() {
                invariant( false );
                return NULL;
            }
            virtual SpecificStats* getSpecificStats() {
                invariant( false );
                return NULL;
            }
            virtual std::vector<PlanStage*> getChildren() const {
                vector<PlanStage*> empty;
                return empty;
            }
            virtual StageType stageType() const {
                return STAGE_NOTIFY_DELETE;
            }
        };

        scoped_ptr<PlanExecutor> _deleteNotifyExec;

    } migrateFromStatus;

    void MigrateFromStatus::DeleteNotificationStage::invalidate(const DiskLoc& dl,
                                                                InvalidationType type) {
        if ( type == INVALIDATION_DELETION ) {
            migrateFromStatus.aboutToDelete( dl );
        }
    }

    struct MigrateStatusHolder {
        MigrateStatusHolder( OperationContext* txn,
                             const std::string& ns ,
                             const BSONObj& min ,
                             const BSONObj& max ,
                             const BSONObj& shardKeyPattern )
                : _txn(txn) {
            _isAnotherMigrationActive = !migrateFromStatus.start(ns, min, max, shardKeyPattern);
        }
        ~MigrateStatusHolder() {
            if (!_isAnotherMigrationActive) {
                migrateFromStatus.done(_txn);
            }
        }

        bool isAnotherMigrationActive() const {
            return _isAnotherMigrationActive;
        }

    private:
        OperationContext* _txn;
        bool _isAnotherMigrationActive;
    };

    void logOpForSharding(OperationContext* txn,
                          const char * opstr,
                          const char * ns,
                          const BSONObj& obj,
                          BSONObj * patt,
                          bool notInActiveChunk) {
        migrateFromStatus.logOp(txn, opstr, ns, obj, patt, notInActiveChunk);
    }

    class TransferModsCommand : public ChunkCommandHelper {
    public:
        void help(stringstream& h) const { h << "internal"; }
        TransferModsCommand() : ChunkCommandHelper( "_transferMods" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return migrateFromStatus.transferMods(txn, errmsg, result);
        }
    } transferModsCommand;


    class InitialCloneCommand : public ChunkCommandHelper {
    public:
        void help(stringstream& h) const { h << "internal"; }
        InitialCloneCommand() : ChunkCommandHelper( "_migrateClone" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            return migrateFromStatus.clone(txn, errmsg, result);
        }
    } initialCloneCommand;

    // Tests can pause / resume moveChunk's progress at each step by enabling / disabling each fail point.
    MONGO_FP_DECLARE(moveChunkHangAtStep1);
    MONGO_FP_DECLARE(moveChunkHangAtStep2);
    MONGO_FP_DECLARE(moveChunkHangAtStep3);
    MONGO_FP_DECLARE(moveChunkHangAtStep4);
    MONGO_FP_DECLARE(moveChunkHangAtStep5);
    MONGO_FP_DECLARE(moveChunkHangAtStep6);

    /**
     * this is the main entry for moveChunk
     * called to initial a move
     * usually by a mongos
     * this is called on the "from" side
     *
     * Format:
     * {
     *   moveChunk: "namespace",
     *   from: "hostAndPort",
     *   fromShard: "shardName",
     *   to: "hostAndPort",
     *   toShard: "shardName",
     *   min: {},
     *   max: {},
     *   maxChunkBytes: numeric,
     *   shardId: "_id of chunk document in config.chunks",
     *   configdb: "hostAndPort",
     *
     *   // optional
     *   secondaryThrottle: bool, //defaults to true.
     *   writeConcern: {} // applies to individual writes.
     * }
     */
    class MoveChunkCommand : public Command {
    public:
        MoveChunkCommand() : Command( "moveChunk" ) {}
        virtual void help( stringstream& help ) const {
            help << "should not be calling this directly";
        }

        virtual bool slaveOk() const { return false; }
        virtual bool adminOnly() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }
        virtual Status checkAuthForCommand(ClientBasic* client,
                                           const std::string& dbname,
                                           const BSONObj& cmdObj) {
            if (!client->getAuthorizationSession()->isAuthorizedForActionsOnResource(
                    ResourcePattern::forExactNamespace(NamespaceString(parseNs(dbname, cmdObj))),
                    ActionType::moveChunk)) {
                return Status(ErrorCodes::Unauthorized, "Unauthorized");
            }
            return Status::OK();
        }
        virtual std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
            return parseNsFullyQualified(dbname, cmdObj);
        }

        bool run(OperationContext* txn, const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            // 1. Parse options
            // 2. Make sure my view is complete and lock the distributed lock to ensure shard
            //    metadata stability.
            // 3. Migration
            //    Retrieve all DiskLocs, which need to be migrated in order to do as little seeking
            //    as possible during transfer. Retrieval of the DiskLocs happens under a collection
            //    lock, but then the collection lock is dropped. This opens up an opportunity for
            //    repair or compact to invalidate these DiskLocs, because these commands do not
            //    synchronized with migration. Note that data modifications are not a problem,
            //    because we are registered for change notifications.
            //
            // 4. pause till migrate caught up
            // 5. LOCK
            //    a) update my config, essentially locking
            //    b) finish migrate
            //    c) update config server
            //    d) logChange to config server
            // 6. wait for all current cursors to expire
            // 7. remove data locally

            // -------------------------------

            // 1.
            string ns = parseNs(dbname, cmdObj);

            // The shard addresses, redundant, but allows for validation
            string toShardHost = cmdObj["to"].str();
            string fromShardHost = cmdObj["from"].str();

            // The shard names
            string toShardName = cmdObj["toShard"].str();
            string fromShardName = cmdObj["fromShard"].str();

            // Process secondary throttle settings and assign defaults if necessary.
            BSONObj secThrottleObj;
            WriteConcernOptions writeConcern;
            Status status = writeConcern.parseSecondaryThrottle(cmdObj, &secThrottleObj);

            if (!status.isOK()){
                if (status.code() != ErrorCodes::WriteConcernNotDefined) {
                    warning() << status.toString() << endl;
                    return appendCommandStatus(result, status);
                }

                writeConcern = getDefaultWriteConcern();
            }
            else {
                repl::ReplicationCoordinator* replCoordinator =
                        repl::getGlobalReplicationCoordinator();

                if (replCoordinator->getReplicationMode() ==
                        repl::ReplicationCoordinator::modeMasterSlave &&
                    writeConcern.shouldWaitForOtherNodes()) {
                    warning() << "moveChunk cannot check if secondary throttle setting "
                              << writeConcern.toBSON()
                              << " can be enforced in a master slave configuration";
                }

                Status status = replCoordinator->checkIfWriteConcernCanBeSatisfied(writeConcern);
                if (!status.isOK() && status != ErrorCodes::NoReplicationEnabled) {
                    warning() << status.toString() << endl;
                    return appendCommandStatus(result, status);
                }
            }

            if (writeConcern.shouldWaitForOtherNodes() &&
                    writeConcern.wTimeout == WriteConcernOptions::kNoTimeout) {
                // Don't allow no timeout.
                writeConcern.wTimeout = kDefaultWTimeoutMs;
            }

            // Do inline deletion
            bool waitForDelete = cmdObj["waitForDelete"].trueValue();
            if (waitForDelete) {
                log() << "moveChunk waiting for full cleanup after move" << endl;
            }

            BSONObj min  = cmdObj["min"].Obj();
            BSONObj max  = cmdObj["max"].Obj();
            BSONElement shardId = cmdObj["shardId"];
            BSONElement maxSizeElem = cmdObj["maxChunkSizeBytes"];

            if ( ns.empty() ) {
                errmsg = "need to specify namespace in command";
                return false;
            }

            if ( toShardName.empty() ) {
                errmsg = "need to specify shard to move chunk to";
                return false;
            }
            if ( fromShardName.empty() ) {
                errmsg = "need to specify shard to move chunk from";
                return false;
            }

            if ( min.isEmpty() ) {
                errmsg = "need to specify a min";
                return false;
            }

            if ( max.isEmpty() ) {
                errmsg = "need to specify a max";
                return false;
            }

            if ( shardId.eoo() ) {
                errmsg = "need shardId";
                return false;
            }

            if ( maxSizeElem.eoo() || ! maxSizeElem.isNumber() ) {
                errmsg = "need to specify maxChunkSizeBytes";
                return false;
            }
            const long long maxChunkSize = maxSizeElem.numberLong(); // in bytes

            // This could be the first call that enables sharding - make sure we initialize the
            // sharding state for this shard.
            if ( ! shardingState.enabled() ) {
                if ( cmdObj["configdb"].type() != String ) {
                    errmsg = "sharding not enabled";
                    warning() << errmsg << endl;
                    return false;
                }
                string configdb = cmdObj["configdb"].String();
                ShardingState::initialize(configdb);
            }

            // Initialize our current shard name in the shard state if needed
            shardingState.gotShardName(fromShardName);

            // Make sure we're as up-to-date as possible with shard information
            // This catches the case where we had to previously changed a shard's host by
            // removing/adding a shard with the same name
            Shard::reloadShardInfo();
            Shard toShard(toShardName);
            Shard fromShard(fromShardName);

            ConnectionString configLoc = ConnectionString::parse(shardingState.getConfigServer(),
                                                                 errmsg);
            if (!configLoc.isValid()) {
                warning() << errmsg;
                return false;
            }

            MoveTimingHelper timing( "from" , ns , min , max , 6 /* steps */ , &errmsg,
                toShardName, fromShardName );

            log() << "received moveChunk request: " << cmdObj << migrateLog;

            timing.done(1);
            MONGO_FP_PAUSE_WHILE(moveChunkHangAtStep1);

            // 2.
            
            if ( migrateFromStatus.isActive() ) {
                errmsg = "migration already in progress";
                warning() << errmsg << endl;
                return false;
            }

            //
            // Get the distributed lock
            //

            ScopedDistributedLock collLock(configLoc, ns);
            collLock.setLockMessage(str::stream() << "migrating chunk [" << minKey << ", " << maxKey
                                                  << ") in " << ns);

            Status acquisitionStatus = collLock.tryAcquire();
            if (!acquisitionStatus.isOK()) {
                errmsg = stream() << "could not acquire collection lock for " << ns
                                  << " to migrate chunk [" << minKey << "," << maxKey << ")"
                                  << causedBy(acquisitionStatus);

                warning() << errmsg << endl;
                return false;
            }

            BSONObj chunkInfo =
                BSON("min" << min << "max" << max <<
                     "from" << fromShard.getName() << "to" << toShard.getName());
            configServer.logChange("moveChunk.start", ns, chunkInfo);

            // Always refresh our metadata remotely
            ChunkVersion origShardVersion;
            Status refreshStatus = shardingState.refreshMetadataNow(txn, ns, &origShardVersion);

            if (!refreshStatus.isOK()) {

                errmsg = str::stream() << "moveChunk cannot start migrate of chunk "
                                       << "[" << minKey << "," << maxKey << ")"
                                       << causedBy(refreshStatus.reason());

                warning() << errmsg;
                return false;
            }

            if (origShardVersion.majorVersion() == 0) {

                // It makes no sense to migrate if our version is zero and we have no chunks
                errmsg = str::stream() << "moveChunk cannot start migrate of chunk "
                                       << "[" << minKey << "," << maxKey << ")"
                                       << " with zero shard version";

                warning() << errmsg;
                return false;
            }

            // From mongos >= v2.8.
            BSONElement epochElem(cmdObj["epoch"]);
            if (epochElem.type() == jstOID) {
                OID cmdEpoch = epochElem.OID();

                if (cmdEpoch != origShardVersion.epoch()) {
                    errmsg = str::stream() << "moveChunk cannot move chunk "
                                           << "[" << minKey << ","
                                           << maxKey << "), "
                                           << "collection may have been dropped. "
                                           << "current epoch: " << origShardVersion.epoch()
                                           << ", cmd epoch: " << cmdEpoch;
                    warning() << errmsg;
                    return false;
                }
            }

            // Get collection metadata
            const CollectionMetadataPtr origCollMetadata(shardingState.getCollectionMetadata(ns));
            // With nonzero shard version, we must have metadata
            invariant(NULL != origCollMetadata);

            ChunkVersion origCollVersion = origCollMetadata->getCollVersion();
            BSONObj shardKeyPattern = origCollMetadata->getKeyPattern();

            // With nonzero shard version, we must have a coll version >= our shard version
            invariant(origCollVersion >= origShardVersion);
            // With nonzero shard version, we must have a shard key
            invariant(!shardKeyPattern.isEmpty());

            ChunkType origChunk;
            if (!origCollMetadata->getNextChunk(min, &origChunk)
                || origChunk.getMin().woCompare(min) || origChunk.getMax().woCompare(max)) {

                // Our boundaries are different from those passed in
                errmsg = str::stream() << "moveChunk cannot find chunk "
                                       << "[" << minKey << "," << maxKey << ")"
                                       << " to migrate, the chunk boundaries may be stale";

                warning() << errmsg;
                return false;
            }

            log() << "moveChunk request accepted at version " << origShardVersion;

            timing.done(2);
            MONGO_FP_PAUSE_WHILE(moveChunkHangAtStep2);

            // 3.
            MigrateStatusHolder statusHolder(txn, ns, min, max, shardKeyPattern);
            
            if (statusHolder.isAnotherMigrationActive()) {
                errmsg = "moveChunk is already in progress from this shard";
                warning() << errmsg << endl;
                return false;
            }

            {
                // See comment at the top of the function for more information on what
                // synchronization is used here.
                if (!migrateFromStatus.storeCurrentLocs(txn, maxChunkSize, errmsg, result)) {
                    warning() << errmsg << endl;
                    return false;
                }

                ScopedDbConnection connTo(toShard.getConnString());
                BSONObj res;
                bool ok;

                const bool isSecondaryThrottle(writeConcern.shouldWaitForOtherNodes());

                BSONObjBuilder recvChunkStartBuilder;
                recvChunkStartBuilder.append("_recvChunkStart", ns);
                recvChunkStartBuilder.append("from", fromShard.getConnString());
                recvChunkStartBuilder.append("fromShardName", fromShard.getName());
                recvChunkStartBuilder.append("toShardName", toShard.getName());
                recvChunkStartBuilder.append("min", min);
                recvChunkStartBuilder.append("max", max);
                recvChunkStartBuilder.append("shardKeyPattern", shardKeyPattern);
                recvChunkStartBuilder.append("configServer", configServer.modelServer());
                recvChunkStartBuilder.append("secondaryThrottle", isSecondaryThrottle);

                // Follow the same convention in moveChunk.
                if (isSecondaryThrottle && !secThrottleObj.isEmpty()) {
                    recvChunkStartBuilder.append("writeConcern", secThrottleObj);
                }

                try{
                    ok = connTo->runCommand("admin", recvChunkStartBuilder.done(), res);
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard "
                                           << toShardName << " to start transfer" << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                connTo.done();

                if ( ! ok ) {
                    errmsg = "moveChunk failed to engage TO-shard in the data transfer: ";
                    verify( res["errmsg"].type() );
                    errmsg += res["errmsg"].String();
                    result.append( "cause" , res );
                    warning() << errmsg << endl;
                    return false;
                }

            }
            timing.done( 3 );
            MONGO_FP_PAUSE_WHILE(moveChunkHangAtStep3);

            // 4.

            // Track last result from TO shard for sanity check
            BSONObj res;
            for ( int i=0; i<86400; i++ ) { // don't want a single chunk move to take more than a day
                invariant(!txn->lockState()->isLocked());

                // Exponential sleep backoff, up to 1024ms. Don't sleep much on the first few
                // iterations, since we want empty chunk migrations to be fast.
                sleepmillis( 1 << std::min( i , 10 ) );
                ScopedDbConnection conn(toShard.getConnString());
                bool ok;
                res = BSONObj();
                try {
                    ok = conn->runCommand( "admin" , BSON( "_recvChunkStatus" << 1 ) , res );
                    res = res.getOwned();
                }
                catch( DBException& e ){
                    errmsg = str::stream() << "moveChunk could not contact to: shard " << toShardName << " to monitor transfer" << causedBy( e );
                    warning() << errmsg << endl;
                    return false;
                }

                conn.done();

                if ( res["ns"].str() != ns ||
                        res["from"].str() != fromShard.getConnString() ||
                        !res["min"].isABSONObj() ||
                        res["min"].Obj().woCompare(min) != 0 ||
                        !res["max"].isABSONObj() ||
                        res["max"].Obj().woCompare(max) != 0 ) {
                    // This can happen when the destination aborted the migration and
                    // received another recvChunk before this thread sees the transition
                    // to the abort state. This is currently possible only if multiple migrations
                    // are happening at once. This is an unfortunate consequence of the shards not
                    // being able to keep track of multiple incoming and outgoing migrations.
                    errmsg = str::stream() << "Destination shard aborted migration, "
                            "now running a new one: " << res;
                    warning() << errmsg << endl;
                    return false;
                }

                LOG(0) << "moveChunk data transfer progress: " << res << " my mem used: " << migrateFromStatus.mbUsed() << migrateLog;

                if ( ! ok || res["state"].String() == "fail" ) {
                    warning() << "moveChunk error transferring data caused migration abort: " << res << migrateLog;
                    errmsg = "data transfer error";
                    result.append( "cause" , res );
                    return false;
                }

                if ( res["state"].String() == "steady" )
                    break;

                if ( migrateFromStatus.mbUsed() > (500 * 1024 * 1024) ) {
                    // this is too much memory for us to use for this
                    // so we're going to abort the migrate
                    ScopedDbConnection conn(toShard.getConnString());

                    BSONObj res;
                    if (!conn->runCommand( "admin", BSON( "_recvChunkAbort" << 1 ), res )) {
                        warning() << "Error encountered while trying to abort migration on "
                                  << "destination shard" << toShard.getConnString() << endl;
                    }

                    res = res.getOwned();
                    conn.done();
                    error() << "aborting migrate because too much memory used res: " << res << migrateLog;
                    errmsg = "aborting migrate because too much memory used";
                    result.appendBool( "split" , true );
                    return false;
                }

                txn->checkForInterrupt();
            }
            timing.done(4);
            MONGO_FP_PAUSE_WHILE(moveChunkHangAtStep4);

            // 5.

            // Before we get into the critical section of the migration, let's double check
            // that the docs have been cloned, the config servers are reachable,
            // and the lock is in place.
            log() << "About to check if it is safe to enter critical section" << endl;

            // Ensure all cloned docs have actually been transferred
            std::size_t locsRemaining = migrateFromStatus.cloneLocsRemaining();
            if ( locsRemaining != 0 ) {

                errmsg =
                    str::stream() << "moveChunk cannot enter critical section before all data is"
                                  << " cloned, " << locsRemaining << " locs were not transferred"
                                  << " but to-shard reported " << res;

                // Should never happen, but safe to abort before critical section
                error() << errmsg << migrateLog;
                dassert( false );
                return false;
            }

            // Ensure distributed lock still held
            Status lockStatus = collLock.checkStatus();
            if (!lockStatus.isOK()) {
                errmsg = str::stream() << "not entering migrate critical section because "
                                       << lockStatus.toString();
                warning() << errmsg << endl;
                return false;
            }

            log() << "About to enter migrate critical section" << endl;

            {
                // 5.a
                // we're under the collection lock here, so no other migrate can change maxVersion
                // or CollectionMetadata state
                migrateFromStatus.setInCriticalSection( true );
                ChunkVersion myVersion = origCollVersion;
                myVersion.incMajor();

                {
                    Lock::DBLock lk(txn->lockState(), nsToDatabaseSubstring(ns), MODE_IX);
                    Lock::CollectionLock collLock(txn->lockState(), ns, MODE_X);
                    verify( myVersion > shardingState.getVersion( ns ) );

                    // bump the metadata's version up and "forget" about the chunk being moved
                    // this is not the commit point but in practice the state in this shard won't
                    // until the commit it done
                    shardingState.donateChunk(txn, ns, min, max, myVersion);
                }

                log() << "moveChunk setting version to: " << myVersion << migrateLog;

                // 5.b
                // we're under the collection lock here, too, so we can undo the chunk donation because no other state change
                // could be ongoing

                BSONObj res;
                bool ok;

                try {
                    ScopedDbConnection connTo( toShard.getConnString(), 35.0 );
                    ok = connTo->runCommand( "admin", BSON( "_recvChunkCommit" << 1 ), res );
                    connTo.done();
                }
                catch ( DBException& e ) {
                    errmsg = str::stream() << "moveChunk could not contact to: shard "
                                           << toShard.getConnString() << " to commit transfer"
                                           << causedBy( e );
                    warning() << errmsg << endl;
                    ok = false;
                }

                if ( !ok || MONGO_FAIL_POINT(failMigrationCommit) ) {
                    log() << "moveChunk migrate commit not accepted by TO-shard: " << res
                          << " resetting shard version to: " << origShardVersion << migrateLog;
                    {
                        Lock::DBLock dbLock(txn->lockState(), nsToDatabaseSubstring(ns), MODE_IX);
                        Lock::CollectionLock collLock(txn->lockState(), ns, MODE_X);

                        log() << "moveChunk global lock acquired to reset shard version from "
                              "failed migration"
                              << endl;

                        // revert the chunk manager back to the state before "forgetting" about the
                        // chunk
                        shardingState.undoDonateChunk(txn, ns, origCollMetadata);
                    }
                    log() << "Shard version successfully reset to clean up failed migration"
                          << endl;

                    errmsg = "_recvChunkCommit failed!";
                    result.append( "cause", res );
                    return false;
                }

                log() << "moveChunk migrate commit accepted by TO-shard: " << res << migrateLog;

                // 5.c

                // version at which the next highest lastmod will be set
                // if the chunk being moved is the last in the shard, nextVersion is that chunk's lastmod
                // otherwise the highest version is from the chunk being bumped on the FROM-shard
                ChunkVersion nextVersion;

                // we want to go only once to the configDB but perhaps change two chunks, the one being migrated and another
                // local one (so to bump version for the entire shard)
                // we use the 'applyOps' mechanism to group the two updates and make them safer
                // TODO pull config update code to a module

                BSONObjBuilder cmdBuilder;

                BSONArrayBuilder updates( cmdBuilder.subarrayStart( "applyOps" ) );
                {
                    // update for the chunk being moved
                    BSONObjBuilder op;
                    op.append( "op" , "u" );
                    op.appendBool( "b" , false /* no upserting */ );
                    op.append( "ns" , ChunkType::ConfigNS );

                    BSONObjBuilder n( op.subobjStart( "o" ) );
                    n.append(ChunkType::name(), Chunk::genID(ns, min));
                    myVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
                    n.append(ChunkType::ns(), ns);
                    n.append(ChunkType::min(), min);
                    n.append(ChunkType::max(), max);
                    n.append(ChunkType::shard(), toShard.getName());
                    n.done();

                    BSONObjBuilder q( op.subobjStart( "o2" ) );
                    q.append(ChunkType::name(), Chunk::genID(ns, min));
                    q.done();

                    updates.append( op.obj() );
                }

                nextVersion = myVersion;

                // if we have chunks left on the FROM shard, update the version of one of them as
                // well.  we can figure that out by grabbing the metadata installed on 5.a

                const CollectionMetadataPtr bumpedCollMetadata( shardingState.getCollectionMetadata( ns ) );
                if( bumpedCollMetadata->getNumChunks() > 0 ) {

                    // get another chunk on that shard
                    ChunkType bumpChunk;
                    bool result = 
                        bumpedCollMetadata->getNextChunk( bumpedCollMetadata->getMinKey(),
                                                          &bumpChunk );
                    BSONObj bumpMin = bumpChunk.getMin();
                    BSONObj bumpMax = bumpChunk.getMax();

                    (void)result; // for compile warning on non-debug
                    dassert( result );
                    dassert( bumpMin.woCompare( min ) != 0 );

                    BSONObjBuilder op;
                    op.append( "op" , "u" );
                    op.appendBool( "b" , false );
                    op.append( "ns" , ChunkType::ConfigNS );

                    nextVersion.incMinor();  // same as used on donateChunk
                    BSONObjBuilder n( op.subobjStart( "o" ) );
                    n.append(ChunkType::name(), Chunk::genID(ns, bumpMin));
                    nextVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
                    n.append(ChunkType::ns(), ns);
                    n.append(ChunkType::min(), bumpMin);
                    n.append(ChunkType::max(), bumpMax);
                    n.append(ChunkType::shard(), fromShard.getName());
                    n.done();

                    BSONObjBuilder q( op.subobjStart( "o2" ) );
                    q.append(ChunkType::name(), Chunk::genID(ns, bumpMin));
                    q.done();

                    updates.append( op.obj() );

                    log() << "moveChunk updating self version to: " << nextVersion << " through "
                          << bumpMin << " -> " << bumpMax << " for collection '" << ns << "'" << migrateLog;

                }
                else {

                    log() << "moveChunk moved last chunk out for collection '" << ns << "'" << migrateLog;
                }

                updates.done();

                BSONArrayBuilder preCond( cmdBuilder.subarrayStart( "preCondition" ) );
                {
                    BSONObjBuilder b;
                    b.append("ns", ChunkType::ConfigNS);
                    b.append("q", BSON("query" << BSON(ChunkType::ns(ns)) <<
                                       "orderby" << BSON(ChunkType::DEPRECATED_lastmod() << -1)));
                    {
                        BSONObjBuilder bb( b.subobjStart( "res" ) );
                        // TODO: For backwards compatibility, we can't yet require an epoch here
                        bb.appendTimestamp(ChunkType::DEPRECATED_lastmod(), origCollVersion.toLong());
                        bb.done();
                    }
                    preCond.append( b.obj() );
                }

                preCond.done();

                BSONObj cmd = cmdBuilder.obj();
                LOG(7) << "moveChunk update: " << cmd << migrateLog;

                int exceptionCode = OkCode;
                ok = false;
                BSONObj cmdResult;
                try {
                    
                    // For testing migration failures
                    if ( MONGO_FAIL_POINT(failMigrationConfigWritePrepare) ) {
                        throw DBException( "mock migration failure before config write",
                                           PrepareConfigsFailedCode );
                    }

                    ScopedDbConnection conn(shardingState.getConfigServer(), 10.0);
                    ok = conn->runCommand( "config" , cmd , cmdResult );

                    if (MONGO_FAIL_POINT(failMigrationApplyOps)) {
                        throw SocketException(SocketException::RECV_ERROR,
                                              shardingState.getConfigServer());
                    }

                    conn.done();
                }
                catch ( DBException& e ) {
                    warning() << e << migrateLog;
                    ok = false;
                    exceptionCode = e.getCode();
                    BSONObjBuilder b;
                    e.getInfo().append( b );
                    cmdResult = b.obj();
                    errmsg = cmdResult.toString();
                }

                if ( exceptionCode == PrepareConfigsFailedCode ) {

                    // In the process of issuing the migrate commit, the SyncClusterConnection
                    // checks that the config servers are reachable. If they are not, we are
                    // sure that the applyOps command was not sent to any of the configs, so we
                    // can safely back out of the migration here, by resetting the shard
                    // version that we bumped up to in the donateChunk() call above.

                    log() << "About to acquire moveChunk global lock to reset shard version from "
                          << "failed migration" << endl;

                    {
                        Lock::DBLock dbLock(txn->lockState(), nsToDatabaseSubstring(ns), MODE_IX);
                        Lock::CollectionLock collLock(txn->lockState(), ns, MODE_X);

                        // Revert the metadata back to the state before "forgetting"
                        // about the chunk.
                        shardingState.undoDonateChunk(txn, ns, origCollMetadata);
                    }

                    log() << "Shard version successfully reset to clean up failed migration" << endl;

                    errmsg = "Failed to send migrate commit to configs because " + errmsg;
                    return false;

                }
                else if ( ! ok || exceptionCode != OkCode ) {

                    // this could be a blip in the connectivity
                    // wait out a few seconds and check if the commit request made it
                    //
                    // if the commit made it to the config, we'll see the chunk in the new shard and there's no action
                    // if the commit did not make it, currently the only way to fix this state is to bounce the mongod so
                    // that the old state (before migrating) be brought in

                    warning() << "moveChunk commit outcome ongoing: " << cmd << " for command :" << cmdResult << migrateLog;
                    sleepsecs( 10 );

                    try {
                        ScopedDbConnection conn(shardingState.getConfigServer(), 10.0);

                        // look for the chunk in this shard whose version got bumped
                        // we assume that if that mod made it to the config, the applyOps was successful
                        BSONObj doc = conn->findOne(ChunkType::ConfigNS,
                                                    Query(BSON(ChunkType::ns(ns)))
                                                        .sort(BSON(ChunkType::DEPRECATED_lastmod() << -1)));


                        ChunkVersion checkVersion(ChunkVersion::fromBSON(doc));
                        if ( checkVersion.equals( nextVersion ) ) {
                            log() << "moveChunk commit confirmed" << migrateLog;
                            errmsg.clear();

                        }
                        else {
                            error() << "moveChunk commit failed: version is at "
                                            << checkVersion << " instead of " << nextVersion << migrateLog;
                            error() << "TERMINATING" << migrateLog;
                            dbexit( EXIT_SHARDING_ERROR );
                        }

                        conn.done();

                    }
                    catch ( ... ) {
                        error() << "moveChunk failed to get confirmation of commit" << migrateLog;
                        error() << "TERMINATING" << migrateLog;
                        dbexit( EXIT_SHARDING_ERROR );
                    }
                }

                migrateFromStatus.setInCriticalSection( false );

                // 5.d
                BSONObjBuilder commitInfo;
                commitInfo.appendElements( chunkInfo );
                if ( res["counts"].type() == Object )
                    commitInfo.appendElements( res["counts"].Obj() );
                configServer.logChange( "moveChunk.commit" , ns , commitInfo.obj() );
            }

            migrateFromStatus.done(txn);
            timing.done(5);
            MONGO_FP_PAUSE_WHILE(moveChunkHangAtStep5);

            // 6.
            // NOTE: It is important that the distributed collection lock be held for this step.
            RangeDeleter* deleter = getDeleter();
            RangeDeleterOptions deleterOptions(KeyRange(ns,
                                                        min.getOwned(),
                                                        max.getOwned(),
                                                        shardKeyPattern));
            deleterOptions.writeConcern = writeConcern;
            deleterOptions.waitForOpenCursors = true;
            deleterOptions.fromMigrate = true;
            deleterOptions.onlyRemoveOrphanedDocs = true;
            deleterOptions.removeSaverReason = "post-cleanup";

            if (waitForDelete) {
                log() << "doing delete inline for cleanup of chunk data" << migrateLog;

                string errMsg;
                // This is an immediate delete, and as a consequence, there could be more
                // deletes happening simultaneously than there are deleter worker threads.
                if (!deleter->deleteNow(txn,
                                        deleterOptions,
                                        &errMsg)) {
                    log() << "Error occured while performing cleanup: " << errMsg << endl;
                }
            }
            else {
                log() << "forking for cleanup of chunk data" << migrateLog;

                string errMsg;
                if (!deleter->queueDelete(deleterOptions,
                                          NULL, // Don't want to be notified.
                                          &errMsg)) {
                    log() << "could not queue migration cleanup: " << errMsg << endl;
                }
            }
            timing.done(6);
            MONGO_FP_PAUSE_WHILE(moveChunkHangAtStep6);

            return true;

        }

    } moveChunkCmd;

    bool ShardingState::inCriticalMigrateSection() {
        return migrateFromStatus.getInCriticalSection();
    }

    bool ShardingState::waitTillNotInCriticalSection( int maxSecondsToWait ) {
        return migrateFromStatus.waitTillNotInCriticalSection( maxSecondsToWait );
    }

    /* -----
       below this are the "to" side commands

       command to initiate
       worker thread
         does initial clone
         pulls initial change set
         keeps pulling
         keeps state
       command to get state
       commend to "commit"
    */

    // Enabling / disabling these fail points pauses / resumes MigrateStatus::_go(), the thread
    // that receives a chunk migration from the donor.
    MONGO_FP_DECLARE(migrateThreadHangAtStep1);
    MONGO_FP_DECLARE(migrateThreadHangAtStep2);
    MONGO_FP_DECLARE(migrateThreadHangAtStep3);
    MONGO_FP_DECLARE(migrateThreadHangAtStep4);
    MONGO_FP_DECLARE(migrateThreadHangAtStep5);

    class MigrateStatus {
    public:
        enum State {
            READY,
            CLONE,
            CATCHUP,
            STEADY,
            COMMIT_START,
            DONE,
            FAIL,
            ABORT
        };

        MigrateStatus():
            m_active("MigrateStatus"),
            active(false),
            stateMutex("migrateStatusStateMutex"),
            state(READY) {
        }

        void setState(State newState) {
            scoped_lock sl(stateMutex);
            state = newState;
        }

        State getState() const {
            scoped_lock sl(stateMutex);
            return state;
        }

        void prepare() {
            scoped_lock l(m_active); // reading and writing 'active'

            verify( ! active );
            setState(READY);
            errmsg = "";

            numCloned = 0;
            clonedBytes = 0;
            numCatchup = 0;
            numSteady = 0;

            active = true;
        }

        void go(OperationContext* txn) {
            try {
                _go(txn);
            }
            catch ( std::exception& e ) {
                setState(FAIL);
                errmsg = e.what();
                error() << "migrate failed: " << e.what() << migrateLog;
            }
            catch ( ... ) {
                setState(FAIL);
                errmsg = "UNKNOWN ERROR";
                error() << "migrate failed with unknown exception" << migrateLog;
            }

            if ( getState() != DONE ) {
                // Unprotect the range if needed/possible on unsuccessful TO migration
                Lock::DBLock dbLock(txn->lockState(), nsToDatabaseSubstring(ns), MODE_IX);
                Lock::CollectionLock collLock(txn->lockState(), ns, MODE_X);

                string errMsg;
                if (!shardingState.forgetPending(txn, ns, min, max, epoch, &errMsg)) {
                    warning() << errMsg << endl;
                }
            }

            setActive( false );
        }

        void _go(OperationContext* txn) {
            verify( getActive() );
            verify( getState() == READY );
            verify( ! min.isEmpty() );
            verify( ! max.isEmpty() );

            log() << "starting receiving-end of migration of chunk " << min << " -> " << max <<
                    " for collection " << ns << " from " << from
                  << " at epoch " << epoch.toString() << endl;

            string errmsg;
            MoveTimingHelper timing( "to" , ns , min , max , 5 /* steps */ , &errmsg, "", "" );

            ScopedDbConnection conn(from);
            conn->getLastError(); // just test connection

            {
                // 0. copy system.namespaces entry if collection doesn't already exist
                Client::WriteContext ctx(txn,  ns );
                // Only copy if ns doesn't already exist
                Database* db = ctx.ctx().db();
                Collection* collection = db->getCollection( txn, ns );

                if ( !collection ) {
                    string system_namespaces = nsToDatabase(ns) + ".system.namespaces";
                    BSONObj entry = conn->findOne( system_namespaces, BSON( "name" << ns ) );
                    BSONObj options;
                    if ( entry["options"].isABSONObj() )
                        options = entry["options"].Obj();

                    WriteUnitOfWork wuow(txn);
                    Status status = userCreateNS( txn, db, ns, options, true, false );
                    if ( !status.isOK() ) {
                        warning() << "failed to create collection [" << ns << "] "
                                  << " with options " << options << ": " << status;
                    }
                    wuow.commit();
                }
            }

            {                
                // 1. copy indexes
                
                vector<BSONObj> indexSpecs;
                {
                    const std::list<BSONObj> indexes = conn->getIndexSpecs(ns);
                    indexSpecs.insert(indexSpecs.begin(), indexes.begin(), indexes.end());
                }

                Lock::DBLock lk(txn->lockState(),  nsToDatabaseSubstring(ns), MODE_X);
                Client::Context ctx(txn,  ns);
                Database* db = ctx.db();
                Collection* collection = db->getCollection( txn, ns );
                if ( !collection ) {
                    errmsg = str::stream() << "collection dropped during migration: " << ns;
                    warning() << errmsg;
                    setState(FAIL);
                    return;
                }

                MultiIndexBlock indexer(txn, collection);

                indexer.removeExistingIndexes(&indexSpecs);

                if (!indexSpecs.empty()) {
                    // Only copy indexes if the collection does not have any documents.
                    if (collection->numRecords(txn) > 0) {
                        errmsg = str::stream() << "aborting migration, shard is missing "
                                               << indexSpecs.size() << " indexes and "
                                               << "collection is not empty. Non-trivial "
                                               << "index creation should be scheduled manually";
                        warning() << errmsg;
                        setState(FAIL);
                        return;
                    }

                    Status status = indexer.init(indexSpecs);
                    if ( !status.isOK() ) {
                        errmsg = str::stream() << "failed to create index before migrating data. "
                                               << " error: " << status.toString();
                        warning() << errmsg;
                        setState(FAIL);
                        return;
                    }

                    status = indexer.insertAllDocumentsInCollection();
                    if ( !status.isOK() ) {
                        errmsg = str::stream() << "failed to create index before migrating data. "
                                               << " error: " << status.toString();
                        warning() << errmsg;
                        setState(FAIL);
                        return;
                    }

                    WriteUnitOfWork wunit(txn);
                    indexer.commit();

                    for (size_t i = 0; i < indexSpecs.size(); i++) {
                        // make sure to create index on secondaries as well
                        repl::logOp(txn, "i", db->getSystemIndexesName().c_str(), indexSpecs[i],
                                       NULL, NULL, true /* fromMigrate */);
                    }

                    wunit.commit();
                }

                timing.done(1);
                MONGO_FP_PAUSE_WHILE(migrateThreadHangAtStep1);
            }

            {
                // 2. delete any data already in range
                RangeDeleterOptions deleterOptions(KeyRange(ns,
                                                            min.getOwned(),
                                                            max.getOwned(),
                                                            shardKeyPattern));
                deleterOptions.writeConcern = writeConcern;
                // No need to wait since all existing cursors will filter out this range when
                // returning the results.
                deleterOptions.waitForOpenCursors = false;
                deleterOptions.fromMigrate = true;
                deleterOptions.onlyRemoveOrphanedDocs = true;
                deleterOptions.removeSaverReason = "preCleanup";

                string errMsg;

                if (!getDeleter()->deleteNow(txn, deleterOptions, &errMsg)) {
                    warning() << "Failed to queue delete for migrate abort: " << errMsg << endl;
                    setState(FAIL);
                    return;
                }

                {
                    // Protect the range by noting that we're now starting a migration to it
                    Lock::DBLock dbLock(txn->lockState(), nsToDatabaseSubstring(ns), MODE_IX);
                    Lock::CollectionLock collLock(txn->lockState(), ns, MODE_X);

                    if (!shardingState.notePending(txn, ns, min, max, epoch, &errmsg)) {
                        warning() << errmsg << endl;
                        setState(FAIL);
                        return;
                    }
                }

                timing.done(2);
                MONGO_FP_PAUSE_WHILE(migrateThreadHangAtStep2);
            }

            State currentState = getState();
            if (currentState == FAIL || currentState == ABORT) {
                string errMsg;
                RangeDeleterOptions deleterOptions(KeyRange(ns,
                                                            min.getOwned(),
                                                            max.getOwned(),
                                                            shardKeyPattern));
                deleterOptions.writeConcern = writeConcern;
                // No need to wait since all existing cursors will filter out this range when
                // returning the results.
                deleterOptions.waitForOpenCursors = false;
                deleterOptions.fromMigrate = true;
                deleterOptions.onlyRemoveOrphanedDocs = true;

                if (!getDeleter()->queueDelete(deleterOptions, NULL /* notifier */, &errMsg)) {
                    warning() << "Failed to queue delete for migrate abort: " << errMsg << endl;
                }
            }

            {
                // 3. initial bulk clone
                setState(CLONE);

                while ( true ) {
                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_migrateClone" << 1 ) , res ) ) {  // gets array of objects to copy, in disk order
                        setState(FAIL);
                        errmsg = "_migrateClone failed: ";
                        errmsg += res.toString();
                        error() << errmsg << migrateLog;
                        conn.done();
                        return;
                    }

                    BSONObj arr = res["objects"].Obj();
                    int thisTime = 0;

                    BSONObjIterator i( arr );
                    while( i.more() ) {
                        txn->checkForInterrupt();

                        if ( getState() == ABORT ) {
                            errmsg = str::stream() << "Migration abort requested while "
                                                   << "copying documents";
                            error() << errmsg << migrateLog;
                            return;
                        }

                        BSONObj o = i.next().Obj();
                        {
                            Client::WriteContext cx(txn, ns );

                            BSONObj localDoc;
                            if ( willOverrideLocalId( txn, cx.ctx().db(), o, &localDoc ) ) {
                                string errMsg =
                                    str::stream() << "cannot migrate chunk, local document "
                                    << localDoc
                                    << " has same _id as cloned "
                                    << "remote document " << o;

                                warning() << errMsg << endl;

                                // Exception will abort migration cleanly
                                uasserted( 16976, errMsg );
                            }

                            Helpers::upsert( txn, ns, o, true );
                        }
                        thisTime++;
                        numCloned++;
                        clonedBytes += o.objsize();

                        if (writeConcern.shouldWaitForOtherNodes() && thisTime > 0) {
                            repl::ReplicationCoordinator::StatusAndDuration replStatus =
                                    repl::getGlobalReplicationCoordinator()->awaitReplication(
                                            txn,
                                            cc().getLastOp(),
                                            writeConcern);
                            if (replStatus.status.code() == ErrorCodes::ExceededTimeLimit) {
                                warning() << "secondaryThrottle on, but doc insert timed out; "
                                             "continuing";
                            }
                            else {
                                massertStatusOK(replStatus.status);
                            }
                        }
                    }

                    if ( thisTime == 0 )
                        break;
                }

                timing.done(3);
                MONGO_FP_PAUSE_WHILE(migrateThreadHangAtStep3);
            }

            // if running on a replicated system, we'll need to flush the docs we cloned to the secondaries
            ReplTime lastOpApplied = cc().getLastOp().asDate();

            {
                // 4. do bulk of mods
                setState(CATCHUP);
                while ( true ) {
                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_transferMods" << 1 ) , res ) ) {
                        setState(FAIL);
                        errmsg = "_transferMods failed: ";
                        errmsg += res.toString();
                        error() << "_transferMods failed: " << res << migrateLog;
                        conn.done();
                        return;
                    }
                    if ( res["size"].number() == 0 )
                        break;

                    apply( txn, res , &lastOpApplied );
                    
                    const int maxIterations = 3600*50;
                    int i;
                    for ( i=0;i<maxIterations; i++) {
                        txn->checkForInterrupt();

                        if ( getState() == ABORT ) {
                            errmsg = str::stream() << "Migration abort requested while waiting "
                                                   << "for replication at catch up stage";
                            error() << errmsg << migrateLog;

                            return;
                        }
                        
                        if (opReplicatedEnough(txn, lastOpApplied, writeConcern))
                            break;
                        
                        if ( i > 100 ) {
                            warning() << "secondaries having hard time keeping up with migrate" << migrateLog;
                        }

                        sleepmillis( 20 );
                    }

                    if ( i == maxIterations ) {
                        errmsg = "secondary can't keep up with migrate";
                        error() << errmsg << migrateLog;
                        conn.done();
                        setState(FAIL);
                        return;
                    } 
                }

                timing.done(4);
                MONGO_FP_PAUSE_WHILE(migrateThreadHangAtStep4);
            }

            { 
                // pause to wait for replication
                // this will prevent us from going into critical section until we're ready
                Timer t;
                while ( t.minutes() < 600 ) {
                    txn->checkForInterrupt();

                    if (getState() == ABORT) {
                        errmsg = "Migration abort requested while waiting for replication";
                        error() << errmsg << migrateLog;
                        return;
                    }

                    log() << "Waiting for replication to catch up before entering critical section"
                          << endl;
                    if (flushPendingWrites(txn, lastOpApplied, writeConcern))
                        break;
                    sleepsecs(1);
                }

                if (t.minutes() >= 600) {
                  setState(FAIL);
                  errmsg = "Cannot go to critical section because secondaries cannot keep up";
                  error() << errmsg << migrateLog;
                  return;
                }
            }

            {
                // 5. wait for commit

                setState(STEADY);
                bool transferAfterCommit = false;
                while ( getState() == STEADY || getState() == COMMIT_START ) {
                    txn->checkForInterrupt();

                    // Make sure we do at least one transfer after recv'ing the commit message
                    // If we aren't sure that at least one transfer happens *after* our state
                    // changes to COMMIT_START, there could be mods still on the FROM shard that
                    // got logged *after* our _transferMods but *before* the critical section.
                    if ( getState() == COMMIT_START ) transferAfterCommit = true;

                    BSONObj res;
                    if ( ! conn->runCommand( "admin" , BSON( "_transferMods" << 1 ) , res ) ) {
                        log() << "_transferMods failed in STEADY state: " << res << migrateLog;
                        errmsg = res.toString();
                        setState(FAIL);
                        conn.done();
                        return;
                    }

                    if ( res["size"].number() > 0 && apply( txn, res , &lastOpApplied ) )
                        continue;

                    if ( getState() == ABORT ) {
                        return;
                    }
                    
                    // We know we're finished when:
                    // 1) The from side has told us that it has locked writes (COMMIT_START)
                    // 2) We've checked at least one more time for un-transmitted mods
                    if ( getState() == COMMIT_START && transferAfterCommit == true ) {
                        if (flushPendingWrites(txn, lastOpApplied, writeConcern))
                            break;
                    }
                    
                    // Only sleep if we aren't committing
                    if ( getState() == STEADY ) sleepmillis( 10 );
                }

                if ( getState() == FAIL ) {
                    errmsg = "timed out waiting for commit";
                    return;
                }

                timing.done(5);
                MONGO_FP_PAUSE_WHILE(migrateThreadHangAtStep5);
            }

            setState(DONE);
            conn.done();
        }

        void status( BSONObjBuilder& b ) {
            b.appendBool( "active" , getActive() );

            b.append( "ns" , ns );
            b.append( "from" , from );
            b.append( "min" , min );
            b.append( "max" , max );
            b.append( "shardKeyPattern" , shardKeyPattern );

            b.append( "state" , stateString() );
            if ( getState() == FAIL )
                b.append( "errmsg" , errmsg );
            {
                BSONObjBuilder bb( b.subobjStart( "counts" ) );
                bb.append( "cloned" , numCloned );
                bb.append( "clonedBytes" , clonedBytes );
                bb.append( "catchup" , numCatchup );
                bb.append( "steady" , numSteady );
                bb.done();
            }


        }

        bool apply( OperationContext* txn, const BSONObj& xfer , ReplTime* lastOpApplied ) {
            ReplTime dummy;
            if ( lastOpApplied == NULL ) {
                lastOpApplied = &dummy;
            }

            bool didAnything = false;

            if ( xfer["deleted"].isABSONObj() ) {
                Lock::DBLock dlk(txn->lockState(), nsToDatabaseSubstring(ns), MODE_IX);
                Helpers::RemoveSaver rs( "moveChunk" , ns , "removedDuring" );

                BSONObjIterator i( xfer["deleted"].Obj() );
                while ( i.more() ) {
                    Lock::CollectionLock clk(txn->lockState(), ns, MODE_X);
                    Client::Context ctx(txn, ns);

                    BSONObj id = i.next().Obj();

                    // do not apply deletes if they do not belong to the chunk being migrated
                    BSONObj fullObj;
                    if (Helpers::findById(txn, ctx.db(), ns.c_str(), id, fullObj)) {
                        if (!isInRange(fullObj , min , max , shardKeyPattern)) {
                            log() << "not applying out of range deletion: " << fullObj << migrateLog;

                            continue;
                        }
                    }

                    if (serverGlobalParams.moveParanoia) {
                        rs.goingToDelete(fullObj);
                    }

                    deleteObjects(txn,
                                  ctx.db(),
                                  ns,
                                  id,
                                  PlanExecutor::YIELD_MANUAL,
                                  true /* justOne */,
                                  true /* logOp */,
                                  false /* god */,
                                  true /* fromMigrate */);

                    *lastOpApplied = ctx.getClient()->getLastOp().asDate();
                    didAnything = true;
                }
            }

            if ( xfer["reload"].isABSONObj() ) {
                BSONObjIterator i( xfer["reload"].Obj() );
                while ( i.more() ) {
                    Client::WriteContext cx(txn, ns);

                    BSONObj it = i.next().Obj();

                    BSONObj localDoc;
                    if ( willOverrideLocalId( txn, cx.ctx().db(), it, &localDoc ) ) {
                        string errMsg =
                            str::stream() << "cannot migrate chunk, local document "
                                          << localDoc
                                          << " has same _id as reloaded remote document "
                                          << it;

                        warning() << errMsg << endl;

                        // Exception will abort migration cleanly
                        uasserted( 16977, errMsg );
                    }

                    // We are in write lock here, so sure we aren't killing
                    Helpers::upsert( txn, ns , it , true );

                    *lastOpApplied = cx.ctx().getClient()->getLastOp().asDate();
                    didAnything = true;
                }
            }

            return didAnything;
        }

        /**
         * Checks if an upsert of a remote document will override a local document with the same _id
         * but in a different range on this shard.
         * Must be in WriteContext to avoid races and DBHelper errors.
         * TODO: Could optimize this check out if sharding on _id.
         */
        bool willOverrideLocalId( OperationContext* txn, Database* db, BSONObj remoteDoc, BSONObj* localDoc ) {

            *localDoc = BSONObj();
            if ( Helpers::findById( txn, db, ns.c_str(), remoteDoc, *localDoc ) ) {
                return !isInRange( *localDoc , min , max , shardKeyPattern );
            }

            return false;
        }

        /**
         * Returns true if the majority of the nodes and the nodes corresponding to the given
         * writeConcern (if not empty) have applied till the specified lastOp.
         */
        bool opReplicatedEnough(const OperationContext* txn,
                                const ReplTime& lastOpApplied,
                                const WriteConcernOptions& writeConcern) {
            WriteConcernOptions majorityWriteConcern;
            majorityWriteConcern.wTimeout = -1;
            majorityWriteConcern.wMode = "majority";
            Status majorityStatus = repl::getGlobalReplicationCoordinator()->awaitReplication(
                    txn, lastOpApplied, majorityWriteConcern).status;

            if (!writeConcern.shouldWaitForOtherNodes()) {
                return majorityStatus.isOK();
            }

            // Also enforce the user specified write concern after "majority" so it covers
            // the union of the 2 write concerns.

            WriteConcernOptions userWriteConcern(writeConcern);
            userWriteConcern.wTimeout = -1;
            Status userStatus = repl::getGlobalReplicationCoordinator()->awaitReplication(
                    txn, lastOpApplied, userWriteConcern).status;

            return majorityStatus.isOK() && userStatus.isOK();
        }

        bool flushPendingWrites(OperationContext* txn,
                                const ReplTime& lastOpApplied,
                                const WriteConcernOptions& writeConcern) {
            if (!opReplicatedEnough(txn, lastOpApplied, writeConcern)) {
                OpTime op( lastOpApplied );
                OCCASIONALLY warning() << "migrate commit waiting for a majority of slaves for '"
                                       << ns << "' " << min << " -> " << max
                                       << " waiting for: " << op
                                       << migrateLog;
                return false;
            }

            log() << "migrate commit succeeded flushing to secondaries for '" << ns << "' " << min << " -> " << max << migrateLog;

            {
                // Get global lock to wait for write to be commited to journal.
                Lock::GlobalRead lk(txn->lockState());

                // if durability is on, force a write to journal
                if (getDur().commitNow(txn)) {
                    log() << "migrate commit flushed to journal for '" << ns << "' " << min << " -> " << max << migrateLog;
                }
            }

            return true;
        }

        string stateString() {
            switch ( getState() ) {
            case READY: return "ready";
            case CLONE: return "clone";
            case CATCHUP: return "catchup";
            case STEADY: return "steady";
            case COMMIT_START: return "commitStart";
            case DONE: return "done";
            case FAIL: return "fail";
            case ABORT: return "abort";
            }
            verify(0);
            return "";
        }

        bool startCommit() {

            if ( getState() != STEADY )
                return false;

            boost::xtime xt;
            boost::xtime_get(&xt, MONGO_BOOST_TIME_UTC);
            xt.sec += 30;

            scoped_lock lock(m_active);
            setState(COMMIT_START);
            while ( active ) {
                if ( ! isActiveCV.timed_wait( lock.boost(), xt ) ){
                    // TIMEOUT
                    setState(FAIL);
                    log() << "startCommit never finished!" << migrateLog;
                    return false;
                }
            }

            if ( getState() == DONE ) {
                return true;
            }

            log() << "startCommit failed, final data failed to transfer" << migrateLog;
            return false;
        }

        void abort() {
            setState(ABORT);
            errmsg = "aborted";
        }

        bool getActive() const { scoped_lock l(m_active); return active; }
        void setActive( bool b ) { 
            scoped_lock l(m_active);
            active = b;
            isActiveCV.notify_all(); 
        }

        // Lock order: m_active -> stateMutex
        mutable mongo::mutex m_active;
        bool active;
        boost::condition isActiveCV;

        string ns;
        string from;

        BSONObj min;
        BSONObj max;
        BSONObj shardKeyPattern;
        OID epoch;

        long long numCloned;
        long long clonedBytes;
        long long numCatchup;
        long long numSteady;
        WriteConcernOptions writeConcern;

        // protects state
        mutable mutex stateMutex;
        State state;
        string errmsg;

    } migrateStatus;

    void migrateThread() {
        Client::initThread( "migrateThread" );
        OperationContextImpl txn;
        if (getGlobalAuthorizationManager()->isAuthEnabled()) {
            ShardedConnectionInfo::addHook();
            cc().getAuthorizationSession()->grantInternalAuthorization();
        }

        // Make curop active so this will show up in currOp.
        cc().curop()->reset();

        migrateStatus.go(&txn);
        cc().shutdown();
    }

    /**
     * Command for initiating the recipient side of the migration to start copying data
     * from the donor shard.
     *
     * {
     *   _recvChunkStart: "namespace",
     *   congfigServer: "hostAndPort",
     *   from: "hostAndPort",
     *   fromShardName: "shardName",
     *   toShardName: "shardName",
     *   min: {},
     *   max: {},
     *   shardKeyPattern: {},
     *
     *   // optional
     *   secondaryThrottle: bool, // defaults to true
     *   writeConcern: {} // applies to individual writes.
     * }
     */
    class RecvChunkStartCommand : public ChunkCommandHelper {
    public:
        void help(stringstream& h) const { h << "internal"; }
        RecvChunkStartCommand() : ChunkCommandHelper( "_recvChunkStart" ) {}

        virtual bool isWriteCommandForConfigServer() const { return false; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {

            // Active state of TO-side migrations (MigrateStatus) is serialized by distributed
            // collection lock.
            if ( migrateStatus.getActive() ) {
                errmsg = "migrate already in progress";
                return false;
            }

            // Pending deletes (for migrations) are serialized by the distributed collection lock,
            // we are sure we registered a delete for a range *before* we can migrate-in a
            // subrange.
            const size_t numDeletes = getDeleter()->getTotalDeletes();
            if (numDeletes > 0) {

                errmsg = str::stream() << "can't accept new chunks because "
                        << " there are still " << numDeletes
                        << " deletes from previous migration";

                warning() << errmsg;
                return false;
            }

            if (!shardingState.enabled()) {
                if (!cmdObj["configServer"].eoo()) {
                    dassert(cmdObj["configServer"].type() == String);
                    ShardingState::initialize(cmdObj["configServer"].String());
                }
                else {

                    errmsg = str::stream()
                        << "cannot start recv'ing chunk, "
                        << "sharding is not enabled and no config server was provided";

                    warning() << errmsg;
                    return false;
                }
            }

            if ( !cmdObj["toShardName"].eoo() ) {
                dassert( cmdObj["toShardName"].type() == String );
                shardingState.gotShardName( cmdObj["toShardName"].String() );
            }

            string ns = cmdObj.firstElement().String();
            BSONObj min = cmdObj["min"].Obj().getOwned();
            BSONObj max = cmdObj["max"].Obj().getOwned();

            // Refresh our collection manager from the config server, we need a collection manager
            // to start registering pending chunks.
            // We force the remote refresh here to make the behavior consistent and predictable,
            // generally we'd refresh anyway, and to be paranoid.
            ChunkVersion currentVersion;
            Status status = shardingState.refreshMetadataNow(txn, ns, &currentVersion );

            if ( !status.isOK() ) {
                errmsg = str::stream() << "cannot start recv'ing chunk "
                                       << "[" << min << "," << max << ")"
                                       << causedBy( status.reason() );

                warning() << errmsg << endl;
                return false;
            }

            migrateStatus.ns = ns;
            migrateStatus.from = cmdObj["from"].String();
            migrateStatus.min = min;
            migrateStatus.max = max;
            migrateStatus.epoch = currentVersion.epoch();

            // Process secondary throttle settings and assign defaults if necessary.
            WriteConcernOptions writeConcern;
            status = writeConcern.parseSecondaryThrottle(cmdObj, NULL);

            if (!status.isOK()){
                if (status.code() != ErrorCodes::WriteConcernNotDefined) {
                    warning() << status.toString() << endl;
                    return appendCommandStatus(result, status);
                }

                writeConcern = getDefaultWriteConcern();
            }
            else {
                repl::ReplicationCoordinator* replCoordinator =
                        repl::getGlobalReplicationCoordinator();

                if (replCoordinator->getReplicationMode() ==
                        repl::ReplicationCoordinator::modeMasterSlave &&
                    writeConcern.shouldWaitForOtherNodes()) {
                    warning() << "recvChunk cannot check if secondary throttle setting "
                              << writeConcern.toBSON()
                              << " can be enforced in a master slave configuration";
                }

                Status status = replCoordinator->checkIfWriteConcernCanBeSatisfied(writeConcern);
                if (!status.isOK() && status != ErrorCodes::NoReplicationEnabled) {
                    warning() << status.toString() << endl;
                    return appendCommandStatus(result, status);
                }
            }

            if (writeConcern.shouldWaitForOtherNodes() &&
                    writeConcern.wTimeout == WriteConcernOptions::kNoTimeout) {
                // Don't allow no timeout.
                writeConcern.wTimeout = kDefaultWTimeoutMs;
            }

            migrateStatus.writeConcern = writeConcern;

            if (cmdObj.hasField("shardKeyPattern")) {
                migrateStatus.shardKeyPattern = cmdObj["shardKeyPattern"].Obj().getOwned();
            } else {
                // shardKeyPattern may not be provided if another shard is from pre 2.2
                // In that case, assume the shard key pattern is the same as the range
                // specifiers provided.
                BSONObj keya = Helpers::inferKeyPattern( migrateStatus.min );
                BSONObj keyb = Helpers::inferKeyPattern( migrateStatus.max );
                verify( keya == keyb );

                warning() << "No shard key pattern provided by source shard for migration."
                    " This is likely because the source shard is running a version prior to 2.2."
                    " Falling back to assuming the shard key matches the pattern of the min and max"
                    " chunk range specifiers.  Inferred shard key: " << keya << endl;

                migrateStatus.shardKeyPattern = keya.getOwned();
            }

            // Set the TO-side migration to active
            migrateStatus.prepare();

            boost::thread m( migrateThread );

            result.appendBool( "started" , true );
            return true;
        }

    } recvChunkStartCmd;

    class RecvChunkStatusCommand : public ChunkCommandHelper {
    public:
        void help(stringstream& h) const { h << "internal"; }
        RecvChunkStatusCommand() : ChunkCommandHelper( "_recvChunkStatus" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            migrateStatus.status( result );
            return 1;
        }

    } recvChunkStatusCommand;

    class RecvChunkCommitCommand : public ChunkCommandHelper {
    public:
        void help(stringstream& h) const { h << "internal"; }
        RecvChunkCommitCommand() : ChunkCommandHelper( "_recvChunkCommit" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            bool ok = migrateStatus.startCommit();
            migrateStatus.status( result );
            return ok;
        }

    } recvChunkCommitCommand;

    class RecvChunkAbortCommand : public ChunkCommandHelper {
    public:
        void help(stringstream& h) const { h << "internal"; }
        RecvChunkAbortCommand() : ChunkCommandHelper( "_recvChunkAbort" ) {}
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool) {
            migrateStatus.abort();
            migrateStatus.status( result );
            return true;
        }

    } recvChunkAboortCommand;


    class IsInRangeTest : public StartupTest {
    public:
        void run() {
            BSONObj min = BSON( "x" << 1 );
            BSONObj max = BSON( "x" << 5 );
            BSONObj skey = BSON( "x" << 1 );

            verify( ! isInRange( BSON( "x" << 0 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 1 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 3 ) , min , max , skey ) );
            verify( isInRange( BSON( "x" << 4 ) , min , max , skey ) );
            verify( ! isInRange( BSON( "x" << 5 ) , min , max , skey ) );
            verify( ! isInRange( BSON( "x" << 6 ) , min , max , skey ) );

            BSONObj obj = BSON( "n" << 3 );
            BSONObj min2 = BSON( "x" << BSONElementHasher::hash64( obj.firstElement() , 0 ) - 2 );
            BSONObj max2 = BSON( "x" << BSONElementHasher::hash64( obj.firstElement() , 0 ) + 2 );
            BSONObj hashedKey =  BSON( "x" << "hashed" );

            verify( isInRange( BSON( "x" << 3 ) , min2 , max2 , hashedKey ) );
            verify( ! isInRange( BSON( "x" << 3 ) , min , max , hashedKey ) );
            verify( ! isInRange( BSON( "x" << 4 ) , min2 , max2 , hashedKey ) );

            LOG(1) << "isInRangeTest passed" << migrateLog;
        }
    } isInRangeTest;
}
