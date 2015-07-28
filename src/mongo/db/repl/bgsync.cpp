/**
 *    Copyright (C) 2012 10gen Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "mongo/db/repl/bgsync.h"

#include "mongo/base/counter.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands/fsync.h"
#include "mongo/db/commands/server_status_metric.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/oplog_interface_local.h"
#include "mongo/db/repl/oplogreader.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/repl/replication_coordinator_impl.h"
#include "mongo/db/repl/rollback_source_impl.h"
#include "mongo/db/repl/rs_rollback.h"
#include "mongo/db/repl/rs_sync.h"
#include "mongo/db/stats/timer_stats.h"
#include "mongo/util/exit.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

using std::string;

namespace repl {

namespace {
const char hashFieldName[] = "h";
int SleepToAllowBatchingMillis = 2;
const int BatchIsSmallish = 40000;  // bytes
}  // namespace

MONGO_FP_DECLARE(rsBgSyncProduce);
MONGO_FP_DECLARE(stepDownWhileDrainingFailPoint);

BackgroundSync* BackgroundSync::s_instance = 0;
stdx::mutex BackgroundSync::s_mutex;

// The number and time spent reading batches off the network
static TimerStats getmoreReplStats;
static ServerStatusMetricField<TimerStats> displayBatchesRecieved("repl.network.getmores",
                                                                  &getmoreReplStats);
// The oplog entries read via the oplog reader
static Counter64 opsReadStats;
static ServerStatusMetricField<Counter64> displayOpsRead("repl.network.ops", &opsReadStats);
// The bytes read via the oplog reader
static Counter64 networkByteStats;
static ServerStatusMetricField<Counter64> displayBytesRead("repl.network.bytes", &networkByteStats);

// The count of items in the buffer
static Counter64 bufferCountGauge;
static ServerStatusMetricField<Counter64> displayBufferCount("repl.buffer.count",
                                                             &bufferCountGauge);
// The size (bytes) of items in the buffer
static Counter64 bufferSizeGauge;
static ServerStatusMetricField<Counter64> displayBufferSize("repl.buffer.sizeBytes",
                                                            &bufferSizeGauge);
// The max size (bytes) of the buffer
static int bufferMaxSizeGauge = 256 * 1024 * 1024;
static ServerStatusMetricField<int> displayBufferMaxSize("repl.buffer.maxSizeBytes",
                                                         &bufferMaxSizeGauge);


BackgroundSyncInterface::~BackgroundSyncInterface() {}

size_t getSize(const BSONObj& o) {
    // SERVER-9808 Avoid Fortify complaint about implicit signed->unsigned conversion
    return static_cast<size_t>(o.objsize());
}

BackgroundSync::BackgroundSync()
    : _buffer(bufferMaxSizeGauge, &getSize),
      _lastOpTimeFetched(Timestamp(std::numeric_limits<int>::max(), 0),
                         std::numeric_limits<long long>::max()),
      _lastFetchedHash(0),
      _pause(true),
      _appliedBuffer(true),
      _replCoord(getGlobalReplicationCoordinator()),
      _initialSyncRequestedFlag(false),
      _indexPrefetchConfig(PREFETCH_ALL) {}

BackgroundSync* BackgroundSync::get() {
    stdx::unique_lock<stdx::mutex> lock(s_mutex);
    if (s_instance == NULL && !inShutdown()) {
        s_instance = new BackgroundSync();
    }
    return s_instance;
}

void BackgroundSync::shutdown() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    // Clear the buffer in case the producerThread is waiting in push() due to a full queue.
    invariant(inShutdown());
    _buffer.clear();
    _pause = true;

    // Wake up producerThread so it notices that we're in shutdown
    _appliedBufferCondition.notify_all();
    _pausedCondition.notify_all();
}

void BackgroundSync::notify(OperationContext* txn) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    // If all ops in the buffer have been applied, unblock waitForRepl (if it's waiting)
    if (_buffer.empty()) {
        _appliedBuffer = true;
        _appliedBufferCondition.notify_all();
    }
}

void BackgroundSync::producerThread(executor::TaskExecutor* taskExecutor) {
    Client::initThread("rsBackgroundSync");
    AuthorizationSession::get(cc())->grantInternalAuthorization();

    while (!inShutdown()) {
        try {
            _producerThread(taskExecutor);
        } catch (const DBException& e) {
            std::string msg(str::stream() << "sync producer problem: " << e.toString());
            error() << msg;
            _replCoord->setMyHeartbeatMessage(msg);
        } catch (const std::exception& e2) {
            severe() << "sync producer exception: " << e2.what();
            fassertFailed(28546);
        }
    }
}

void BackgroundSync::_producerThread(executor::TaskExecutor* taskExecutor) {
    const MemberState state = _replCoord->getMemberState();
    // we want to pause when the state changes to primary
    if (_replCoord->isWaitingForApplierToDrain() || state.primary()) {
        if (!_pause) {
            stop();
        }
        sleepsecs(1);
        return;
    }

    // TODO(spencer): Use a condition variable to await loading a config.
    if (state.startup()) {
        // Wait for a config to be loaded
        sleepsecs(1);
        return;
    }

    // We need to wait until initial sync has started.
    if (_replCoord->getMyLastOptime().isNull()) {
        sleepsecs(1);
        return;
    }
    // we want to unpause when we're no longer primary
    // start() also loads _lastOpTimeFetched, which we know is set from the "if"
    OperationContextImpl txn;
    if (_pause) {
        start(&txn);
    }

    _produce(&txn, taskExecutor);
}

void BackgroundSync::_produce(OperationContext* txn, executor::TaskExecutor* taskExecutor) {
    // this oplog reader does not do a handshake because we don't want the server it's syncing
    // from to track how far it has synced
    {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        if (_lastOpTimeFetched.isNull()) {
            // then we're initial syncing and we're still waiting for this to be set
            lock.unlock();
            sleepsecs(1);
            // if there is no one to sync from
            return;
        }

        if (_replCoord->isWaitingForApplierToDrain() || _replCoord->getMemberState().primary() ||
            inShutdownStrict()) {
            return;
        }
    }

    while (MONGO_FAIL_POINT(rsBgSyncProduce)) {
        sleepmillis(0);
    }


    // find a target to sync from the last optime fetched
    OpTime lastOpTimeFetched;
    {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        lastOpTimeFetched = _lastOpTimeFetched;
        _syncSourceHost = HostAndPort();
    }
    OplogReader syncSourceReader;
    syncSourceReader.connectToSyncSource(txn, lastOpTimeFetched, _replCoord);

    {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        // no server found
        if (syncSourceReader.getHost().empty()) {
            lock.unlock();
            sleepsecs(1);
            // if there is no one to sync from
            return;
        }
        lastOpTimeFetched = _lastOpTimeFetched;
        _syncSourceHost = syncSourceReader.getHost();
        _replCoord->signalUpstreamUpdater();
    }

    syncSourceReader.tailingQueryGTE(rsOplogName.c_str(), lastOpTimeFetched.getTimestamp());

    // if target cut connections between connecting and querying (for
    // example, because it stepped down) we might not have a cursor
    if (!syncSourceReader.haveCursor()) {
        return;
    }

    {
        // Prefer host in oplog reader to _syncSourceHost because _syncSourceHost may be cleared
        // if sync source feedback fails.
        const HostAndPort source = syncSourceReader.getHost();

        auto getNextOperation = [&syncSourceReader]() -> StatusWith<BSONObj> {
            if (!syncSourceReader.more()) {
                return Status(ErrorCodes::OplogStartMissing, "remote oplog start missing");
            }
            return syncSourceReader.nextSafe();
        };
        auto getConnection =
            [&syncSourceReader]() -> DBClientBase* { return syncSourceReader.conn(); };

        auto remoteOplogStartStatus = _checkRemoteOplogStart(getNextOperation);
        if (!remoteOplogStartStatus.isOK()) {
            log() << "starting rollback: " << remoteOplogStartStatus;
            _rollback(txn, source, getConnection);
            stop();
            return;
        }
    }

    while (!inShutdown()) {
        if (!syncSourceReader.moreInCurrentBatch()) {
            // Check some things periodically
            // (whenever we run out of items in the
            // current cursor batch)

            int bs = syncSourceReader.currentBatchMessageSize();
            if (bs > 0 && bs < BatchIsSmallish) {
                // on a very low latency network, if we don't wait a little, we'll be
                // getting ops to write almost one at a time.  this will both be expensive
                // for the upstream server as well as potentially defeating our parallel
                // application of batches on the secondary.
                //
                // the inference here is basically if the batch is really small, we are
                // "caught up".
                //
                sleepmillis(SleepToAllowBatchingMillis);
            }

            // If we are transitioning to primary state, we need to leave
            // this loop in order to go into bgsync-pause mode.
            if (_replCoord->isWaitingForApplierToDrain() ||
                _replCoord->getMemberState().primary()) {
                return;
            }

            // re-evaluate quality of sync target
            if (_shouldChangeSyncSource(syncSourceReader.getHost())) {
                return;
            }

            {
                // record time for each getmore
                TimerHolder batchTimer(&getmoreReplStats);

                // This calls receiveMore() on the oplogreader cursor.
                // It can wait up to five seconds for more data.
                syncSourceReader.more();
            }
            networkByteStats.increment(syncSourceReader.currentBatchMessageSize());

            if (!syncSourceReader.moreInCurrentBatch()) {
                // If there is still no data from upstream, check a few more things
                // and then loop back for another pass at getting more data
                {
                    stdx::unique_lock<stdx::mutex> lock(_mutex);
                    if (_pause) {
                        return;
                    }
                }

                syncSourceReader.tailCheck();
                if (!syncSourceReader.haveCursor()) {
                    LOG(1) << "replSet end syncTail pass";
                    return;
                }

                continue;
            }
        }

        // If we are transitioning to primary state, we need to leave
        // this loop in order to go into bgsync-pause mode.
        if (_replCoord->isWaitingForApplierToDrain() || _replCoord->getMemberState().primary()) {
            LOG(1) << "waiting for draining or we are primary, not adding more ops to buffer";
            return;
        }

        // At this point, we are guaranteed to have at least one thing to read out
        // of the oplogreader cursor.
        BSONObj o = syncSourceReader.nextSafe().getOwned();
        opsReadStats.increment();

        if (MONGO_FAIL_POINT(stepDownWhileDrainingFailPoint)) {
            sleepsecs(20);
        }

        {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            _appliedBuffer = false;
        }

        OCCASIONALLY {
            LOG(2) << "bgsync buffer has " << _buffer.size() << " bytes";
        }

        bufferCountGauge.increment();
        bufferSizeGauge.increment(getSize(o));
        _buffer.push(o);

        {
            stdx::unique_lock<stdx::mutex> lock(_mutex);
            _lastFetchedHash = o["h"].numberLong();
            _lastOpTimeFetched = extractOpTime(o);
            LOG(3) << "lastOpTimeFetched: " << _lastOpTimeFetched;
        }
    }
}

bool BackgroundSync::_shouldChangeSyncSource(const HostAndPort& syncSource) {
    // is it even still around?
    if (getSyncTarget().empty() || syncSource.empty()) {
        return true;
    }

    // check other members: is any member's optime more than MaxSyncSourceLag seconds
    // ahead of the current sync source?
    return _replCoord->shouldChangeSyncSource(syncSource);
}


bool BackgroundSync::peek(BSONObj* op) {
    return _buffer.peek(*op);
}

void BackgroundSync::waitForMore() {
    BSONObj op;
    // Block for one second before timing out.
    // Ignore the value of the op we peeked at.
    _buffer.blockingPeek(op, 1);
}

void BackgroundSync::consume() {
    // this is just to get the op off the queue, it's been peeked at
    // and queued for application already
    BSONObj op = _buffer.blockingPop();
    bufferCountGauge.decrement(1);
    bufferSizeGauge.decrement(getSize(op));
}

Status BackgroundSync::_checkRemoteOplogStart(
    stdx::function<StatusWith<BSONObj>()> getNextOperation) {
    auto result = getNextOperation();
    if (!result.isOK()) {
        // The GTE query from upstream returns nothing, so we're ahead of the upstream.
        return Status(ErrorCodes::RemoteOplogStale,
                      "we are ahead of the sync source, will try to roll back");
    }
    BSONObj o = result.getValue();
    OpTime opTime = extractOpTime(o);
    long long hash = o["h"].numberLong();
    if (opTime != _lastOpTimeFetched || hash != _lastFetchedHash) {
        return Status(ErrorCodes::OplogStartMissing,
                      str::stream() << "our last op time fetched: " << _lastOpTimeFetched.toString()
                                    << ". source's GTE: " << opTime.toString());
    }
    return Status::OK();
}

void BackgroundSync::_rollback(OperationContext* txn,
                               const HostAndPort& source,
                               stdx::function<DBClientBase*()> getConnection) {
    // Abort only when syncRollback detects we are in a unrecoverable state.
    // In other cases, we log the message contained in the error status and retry later.
    auto status = syncRollback(txn,
                               _replCoord->getMyLastOptime(),
                               OplogInterfaceLocal(txn, rsOplogName),
                               RollbackSourceImpl(getConnection, source, rsOplogName),
                               _replCoord);
    if (status.isOK()) {
        return;
    }
    if (ErrorCodes::UnrecoverableRollbackError == status.code()) {
        fassertNoTrace(28723, status);
    }
    warning() << "rollback cannot proceed at this time (retrying later): " << status;
}

HostAndPort BackgroundSync::getSyncTarget() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    return _syncSourceHost;
}

void BackgroundSync::clearSyncTarget() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    _syncSourceHost = HostAndPort();
}

void BackgroundSync::stop() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    _pause = true;
    _syncSourceHost = HostAndPort();
    _lastOpTimeFetched = OpTime();
    _lastFetchedHash = 0;
    _appliedBufferCondition.notify_all();
    _pausedCondition.notify_all();
}

void BackgroundSync::start(OperationContext* txn) {
    massert(16235, "going to start syncing, but buffer is not empty", _buffer.empty());

    long long lastFetchedHash = _readLastAppliedHash(txn);
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _pause = false;

    // reset _last fields with current oplog data
    _lastOpTimeFetched = _replCoord->getMyLastOptime();
    _lastFetchedHash = lastFetchedHash;

    LOG(1) << "bgsync fetch queue set to: " << _lastOpTimeFetched << " " << _lastFetchedHash;
}

void BackgroundSync::waitUntilPaused() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    while (!_pause) {
        _pausedCondition.wait(lock);
    }
}

void BackgroundSync::clearBuffer() {
    _buffer.clear();
}

long long BackgroundSync::_readLastAppliedHash(OperationContext* txn) {
    BSONObj oplogEntry;
    try {
        MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
            ScopedTransaction transaction(txn, MODE_IX);
            Lock::DBLock lk(txn->lockState(), "local", MODE_X);
            bool success = Helpers::getLast(txn, rsOplogName.c_str(), oplogEntry);
            if (!success) {
                // This can happen when we are to do an initial sync.  lastHash will be set
                // after the initial sync is complete.
                return 0;
            }
        }
        MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "readLastAppliedHash", rsOplogName);
    } catch (const DBException& ex) {
        severe() << "Problem reading " << rsOplogName << ": " << ex.toStatus();
        fassertFailed(18904);
    }
    BSONElement hashElement = oplogEntry[hashFieldName];
    if (hashElement.eoo()) {
        severe() << "Most recent entry in " << rsOplogName << " missing \"" << hashFieldName
                 << "\" field";
        fassertFailed(18902);
    }
    if (hashElement.type() != NumberLong) {
        severe() << "Expected type of \"" << hashFieldName << "\" in most recent " << rsOplogName
                 << " entry to have type NumberLong, but found " << typeName(hashElement.type());
        fassertFailed(18903);
    }
    return hashElement.safeNumberLong();
}

bool BackgroundSync::getInitialSyncRequestedFlag() {
    stdx::lock_guard<stdx::mutex> lock(_initialSyncMutex);
    return _initialSyncRequestedFlag;
}

void BackgroundSync::setInitialSyncRequestedFlag(bool value) {
    stdx::lock_guard<stdx::mutex> lock(_initialSyncMutex);
    _initialSyncRequestedFlag = value;
}

void BackgroundSync::pushTestOpToBuffer(const BSONObj& op) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _buffer.push(op);
}


}  // namespace repl
}  // namespace mongo
