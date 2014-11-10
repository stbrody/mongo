/**
*    Copyright (C) 2008 10gen Inc.
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

#include "mongo/db/repl/rs.h"

#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/repl/bgsync.h"
#include "mongo/db/repl/connections.h"
#include "mongo/db/repl/repl_set_impl.h"
#include "mongo/db/repl/rslog.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/log.h"

namespace mongo {
namespace repl {
    
    ReplSet *theReplSet = 0;

    // This is a bitmask with the first bit set. It's used to mark connections that should be kept
    // open during stepdowns
    const unsigned ScopedConn::keepOpen = 1;

    ReplSet::ReplSet() {
    }

    ReplSet* ReplSet::make(OperationContext* txn, ReplSetSeedList& replSetSeedList) {
        auto_ptr<ReplSet> ret(new ReplSet());
        ret->init(txn, replSetSeedList);
        return ret.release();
    }

    ReplSetImpl::StartupStatus ReplSetImpl::startupStatus = PRESTART;
    DiagStr ReplSetImpl::startupStatusMsg;

    void ReplSet::haveNewConfig(OperationContext* txn, ReplSetConfig& newConfig, bool addComment) {
        bo comment;
        if( addComment )
            comment = BSON( "msg" << "Reconfig set" << "version" << newConfig.version );

        newConfig.saveConfigLocally(txn, comment);

        try {
            BSONObj oldConfForAudit = config().asBson();
            BSONObj newConfForAudit = newConfig.asBson();
            audit::logReplSetReconfig(ClientBasic::getCurrent(),
                                      &oldConfForAudit,
                                      &newConfForAudit);
            if (initFromConfig(txn, newConfig, true)) {
                log() << "replSet replSetReconfig new config saved locally" << rsLog;
            }
        }
        catch (const DBException& e) {
            log() << "replSet error unexpected exception in haveNewConfig() : " << e.toString() << rsLog;
            fassertFailedNoTrace(18755);
        }
        catch (...) {
            std::terminate();
        }
    }

    void Manager::msgReceivedNewConfig(BSONObj o) {
        OperationContextImpl txn;

        log() << "replset msgReceivedNewConfig version: " << o["version"].toString() << rsLog;
        scoped_ptr<ReplSetConfig> config(ReplSetConfig::make(&txn, o));
        if( config->version > rs->config().version )
            theReplSet->haveNewConfig(&txn, *config, false);
        else {
            log() << "replSet info msgReceivedNewConfig but version isn't higher " <<
                  config->version << ' ' << rs->config().version << rsLog;
        }
    }

    /* forked as a thread during startup
       it can run quite a while looking for config.  but once found,
       a separate thread takes over as ReplSetImpl::Manager, and this thread
       terminates.
    */
    void startReplSets(ReplSetSeedList *replSetSeedList) {
        Client::initThread("rsStart");
        OperationContextImpl txn;

        try {
            verify( theReplSet == 0 );
            if( replSetSeedList == 0 ) {
                return;
            }
            cc().getAuthorizationSession()->grantInternalAuthorization();
            (theReplSet = ReplSet::make(&txn, *replSetSeedList))->go();
        }
        catch(std::exception& e) {
            log() << "replSet caught exception in startReplSets thread: " << e.what() << rsLog;
            if( theReplSet )
                fassertFailedNoTrace(18756);
        }
        cc().shutdown();
    }

} // namespace repl
} // namespace mongo
