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

#include "mongo/db/auth/authz_manager_external_state_d.h"

#include <boost/thread/mutex.hpp>
#include <string>

#include "mongo/base/status.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"
#include "mongo/db/d_concurrency.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/instance.h"
#include "mongo/db/jsobj.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {
    static Status userNotFoundStatus(ErrorCodes::UserNotFound, "User not found");
}

    AuthzManagerExternalStateMongod::AuthzManagerExternalStateMongod() {}
    AuthzManagerExternalStateMongod::~AuthzManagerExternalStateMongod() {}

    Status AuthzManagerExternalStateMongod::_findUser(const string& usersNamespace,
                                                      const BSONObj& query,
                                                      BSONObj* result) {
        Client::GodScope gs;
        Client::ReadContext ctx(usersNamespace);

        if (!Helpers::findOne(usersNamespace, query, *result)) {
            return userNotFoundStatus;
        }
        return Status::OK();
    }

    Status AuthzManagerExternalStateMongod::query(
            const NamespaceString& collectionName,
            const BSONObj& query,
            const boost::function<void(const BSONObj&)>& resultProcessor) {
        try {
            DBDirectClient client;
            Client::GodScope gs;
            client.query(resultProcessor, collectionName.ns(), query);
            return Status::OK();
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::getAllDatabaseNames(
            std::vector<std::string>* dbnames) {
        Lock::GlobalRead lk;
        getDatabaseNames(*dbnames);
        return Status::OK();
    }

    Status AuthzManagerExternalStateMongod::getAllV1PrivilegeDocsForDB(
            const std::string& dbname, std::vector<BSONObj>* privDocs) {
        std::string usersNamespace = dbname + ".system.users";

        Client::GodScope gs;
        Client::ReadContext ctx(usersNamespace);

        *privDocs = Helpers::findAll(usersNamespace, BSONObj());
        return Status::OK();
    }

    Status AuthzManagerExternalStateMongod::findOne(
            const NamespaceString& collectionName,
            const BSONObj& query,
            BSONObj* result) {
        fassertFailed(17091);
    }

    Status AuthzManagerExternalStateMongod::insert(
            const NamespaceString& collectionName,
            const BSONObj& document,
            const BSONObj& writeConcern) {
        try {
            DBDirectClient client;
            {
                Client::GodScope gs;
                // TODO(spencer): Once we're no longer fully rebuilding the user cache on every
                // change to user data we should remove the global lock and uncomment the
                // WriteContext below
                Lock::GlobalWrite w;
                // Client::WriteContext ctx(userNS);
                client.insert(collectionName, document);
            }

            // Handle write concern
            BSONObjBuilder gleBuilder;
            gleBuilder.append("getLastError", 1);
            gleBuilder.appendElements(writeConcern);
            BSONObj res;
            client.runCommand("admin", gleBuilder.done(), res);
            string errstr = client.getLastErrorString(res);
            if (errstr.empty()) {
                return Status::OK();
            }
            if (res.hasField("code") && res["code"].Int() == ASSERT_ID_DUPKEY) {
                return Status(ErrorCodes::DuplicateKey, errstr);
            }
            return Status(ErrorCodes::UnknownError, errstr);
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::updateOne(
            const NamespaceString& collectionName,
            const BSONObj& query,
            const BSONObj& updatePattern,
            bool upsert,
            const BSONObj& writeConcern) {
        try {
            DBDirectClient client;
            {
                Client::GodScope gs;
                // TODO(spencer): Once we're no longer fully rebuilding the user cache on every
                // change to user data we should remove the global lock and uncomment the
                // WriteContext below
                Lock::GlobalWrite w;
                // Client::WriteContext ctx(userNS);
                client.update(collectionName, query, updatePattern, upsert);
            }

            // Handle write concern
            BSONObjBuilder gleBuilder;
            gleBuilder.append("getLastError", 1);
            gleBuilder.appendElements(writeConcern);
            BSONObj res;
            client.runCommand("admin", gleBuilder.done(), res);
            string err = client.getLastErrorString(res);
            if (!err.empty()) {
                return Status(ErrorCodes::UnknownError, err);
            }

            int numUpdated = res["n"].numberInt();
            dassert(numUpdated <= 1 && numUpdated >= 0);
            if (numUpdated == 0) {
                return Status(ErrorCodes::NoMatchingDocument, "No document found");
            }

            return Status::OK();
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::remove(
            const NamespaceString& collectionName,
            const BSONObj& query,
            const BSONObj& writeConcern,
            int* numRemoved) {
        try {
            DBDirectClient client;
            {
                Client::GodScope gs;
                // TODO(spencer): Once we're no longer fully rebuilding the user cache on every
                // change to user data we should remove the global lock and uncomment the
                // WriteContext below
                Lock::GlobalWrite w;
                // Client::WriteContext ctx(userNS);
                client.remove(collectionName, query);
            }

            // Handle write concern
            BSONObjBuilder gleBuilder;
            gleBuilder.append("getLastError", 1);
            gleBuilder.appendElements(writeConcern);
            BSONObj res;
            client.runCommand("admin", gleBuilder.done(), res);
            string errstr = client.getLastErrorString(res);
            if (!errstr.empty()) {
                return Status(ErrorCodes::UnknownError, errstr);
            }

            *numRemoved = res["n"].numberInt();
            return Status::OK();
        } catch (const DBException& e) {
            return e.toStatus();
        }
    }

    Status AuthzManagerExternalStateMongod::createIndex(
            const NamespaceString& collectionName,
            const BSONObj& pattern,
            bool unique,
            const BSONObj& writeConcern) {
        fassertFailed(17095);
    }

    Status AuthzManagerExternalStateMongod::dropCollection(
            const NamespaceString& collectionName,
            const BSONObj& writeConcern) {
        fassertFailed(17096);
    }

    Status AuthzManagerExternalStateMongod::renameCollection(
            const NamespaceString& oldName,
            const NamespaceString& newName,
            const BSONObj& writeConcern) {
        fassertFailed(17097);
    }

    Status AuthzManagerExternalStateMongod::copyCollection(
            const NamespaceString& fromName,
            const NamespaceString& toName,
            const BSONObj& writeConcern) {
        fassertFailed(17098);
    }

    bool AuthzManagerExternalStateMongod::tryAcquireAuthzUpdateLock(const StringData& why) {
        log() << "Attempting to lock user data for: " << why << endl;
        return _authzDataUpdateLock.try_lock();
    }

    void AuthzManagerExternalStateMongod::releaseAuthzUpdateLock() {
        return _authzDataUpdateLock.unlock();
    }

    OpTime AuthzManagerExternalStateMongod::getCurrentOpTime() {
        mongo::mutex::scoped_lock lk(OpTime::m);
        return OpTime::now(lk);
    }

    static const NamespaceString oplogCollectionName("local.oplog.rs");

    const NamespaceString& AuthzManagerExternalStateMongod::getOplogCollectionName() {
        return oplogCollectionName;
    }

} // namespace mongo
