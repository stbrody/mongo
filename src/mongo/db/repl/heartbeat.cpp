/**
*    Copyright (C) 2008 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,b
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
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include <boost/thread/thread.hpp>

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/commands.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/global_environment_experiment.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/connections.h"
#include "mongo/db/repl/heartbeat_info.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/repl_coordinator_global.h"
#include "mongo/db/repl/repl_set_health_poll_task.h"
#include "mongo/db/repl/repl_set_heartbeat_args.h"
#include "mongo/db/repl/repl_set_heartbeat_response.h"
#include "mongo/db/repl/replset_commands.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl/server.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/util/background.h"
#include "mongo/util/concurrency/task.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/goodies.h"
#include "mongo/util/log.h"
#include "mongo/util/ramlog.h"

namespace mongo {
namespace repl {

    MONGO_FP_DECLARE(rsDelayHeartbeatResponse);

namespace {
    /**
     * Returns true if there is no data on this server. Useful when starting replication.
     * The "local" database does NOT count except for "rs.oplog" collection.
     * Used to set the hasData field on replset heartbeat command response.
     */
    bool replHasDatabases(OperationContext* txn) {
        vector<string> names;
        StorageEngine* storageEngine = getGlobalEnvironment()->getGlobalStorageEngine();
        storageEngine->listDatabases(&names);

        if( names.size() >= 2 ) return true;
        if( names.size() == 1 ) {
            if( names[0] != "local" )
                return true;
            // we have a local database.  return true if oplog isn't empty
            {
                Lock::DBRead lk(txn->lockState(), repl::rsoplog);
                BSONObj o;
                if( Helpers::getFirst(txn, repl::rsoplog, o) )
                    return true;
            }
        }
        return false;
    }
    
} // namespace

    /* { replSetHeartbeat : <setname> } */
    class CmdReplSetHeartbeat : public ReplSetCommand {
    public:
        void help(stringstream& h) const { h << "internal"; }
        CmdReplSetHeartbeat() : ReplSetCommand("replSetHeartbeat") { }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::internal);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        virtual bool run(OperationContext* txn, const string& , BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {

            MONGO_FAIL_POINT_BLOCK(rsDelayHeartbeatResponse, delay) {
                const BSONObj& data = delay.getData();
                sleepsecs(data["delay"].numberInt());
            }

            /* we don't call ReplSetCommand::check() here because heartbeat
               checks many things that are pre-initialization. */
            if (!getGlobalReplicationCoordinator()->getSettings().usingReplSets()) {
                errmsg = "not running with --replSet";
                return false;
            }

            if ( replSetBlind ) {
                errmsg = str::stream() << "node is blind";
                return false;
            }

            /* we want to keep heartbeat connections open when relinquishing primary.  
               tag them here. */
            {
                AbstractMessagingPort *mp = cc().port();
                if( mp )
                    mp->tag |= ScopedConn::keepOpen;
            }

            ReplSetHeartbeatArgs args;
            Status status = args.initialize(cmdObj);
            if (!status.isOK()) {
                return appendCommandStatus(result, status);
            }

            // ugh.
            if (args.getCheckEmpty()) {
                result.append("hasData", replHasDatabases(txn));
            }

            ReplSetHeartbeatResponse response;
            status = getGlobalReplicationCoordinator()->processHeartbeat(args, &response);
            if (status.isOK())
                response.addToBSON(&result);
            return appendCommandStatus(result, status);
        }
    } cmdReplSetHeartbeat;

    MONGO_FP_DECLARE(rsStopHeartbeatRequest);

    bool requestHeartbeat(const std::string& setName,
                          const std::string& from,
                          const std::string& memberFullName,
                          BSONObj& result,
                          int myCfgVersion,
                          int& theirCfgVersion,
                          bool checkEmpty) {
        if( replSetBlind ) {
            return false;
        }

        MONGO_FAIL_POINT_BLOCK(rsStopHeartbeatRequest, member) {
            const BSONObj& data = member.getData();
            const std::string& stopMember = data["member"].str();

            if (memberFullName == stopMember) {
                return false;
            }
        }
        int me = -1;
        if (theReplSet) {
            me = theReplSet->selfId();
        }

        BSONObjBuilder cmdBuilder;
        cmdBuilder.append("replSetHeartbeat", setName);
        cmdBuilder.append("v", myCfgVersion);
        cmdBuilder.append("pv", 1);
        cmdBuilder.append("checkEmpty", checkEmpty);
        cmdBuilder.append("from", from);
        if (me > -1) {
            cmdBuilder.append("fromId", me);
        }

        ScopedConn conn(memberFullName);
        return conn.runCommand("admin", cmdBuilder.done(), result, 0);
    }

    void ReplSetImpl::endOldHealthTasks() {
        unsigned sz = healthTasks.size();
        for( set<ReplSetHealthPollTask*>::iterator i = healthTasks.begin(); i != healthTasks.end(); i++ )
            (*i)->halt();
        healthTasks.clear();
        if( sz )
            DEV log() << "replSet debug: cleared old tasks " << sz << endl;
    }

    void ReplSetImpl::startHealthTaskFor(Member *m) {
        DEV log() << "starting rsHealthPoll for " << m->fullName() << endl;
        ReplSetHealthPollTask *task = new ReplSetHealthPollTask(m->h(), m->hbinfo());
        healthTasks.insert(task);
        task::repeat(task, 2000);
    }

    void startSyncThread();

    /** called during repl set startup.  caller expects it to return fairly quickly.
        note ReplSet object is only created once we get a config - so this won't run
        until the initiation.
    */
    void ReplSetImpl::startThreads() {
        task::fork(mgr);
        mgr->send( stdx::bind(&Manager::msgCheckNewState, theReplSet->mgr) );

        if (myConfig().arbiterOnly) {
            return;
        }

        // this ensures that will have bgsync's s_instance at all points where it is needed
        // so that we needn't check for its existence
        BackgroundSync* sync = BackgroundSync::get();

        boost::thread t(startSyncThread);

        boost::thread producer(stdx::bind(&BackgroundSync::producerThread, sync));
        //boost::thread feedback(stdx::bind(&SyncSourceFeedback::run,
        //                                  &theReplSet->syncSourceFeedback));

        // member heartbeats are started in ReplSetImpl::initFromConfig
    }

} // namespace repl
} // namespace mongo

/* todo:
   stop bg job and delete on removefromset
*/
