/*
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
*/

#pragma once

#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    /**
     * Public interface for a class that encapsulates all the information related to system
     * state not stored in AuthorizationManager.  This is primarily to make AuthorizationManager
     * easier to test as well as to allow different implementations for mongos and mongod.
     */
    class AuthzManagerExternalState {
        MONGO_DISALLOW_COPYING(AuthzManagerExternalState);

    public:

        virtual ~AuthzManagerExternalState();

        // Gets the privilege information document for "userName".
        //
        // On success, returns Status::OK() and stores a shared-ownership copy of the document into
        // "result".
        Status getPrivilegeDocument(const UserName& userName, BSONObj* result) const;

        // Returns true if there exists at least one privilege document in the system.
        bool hasAnyPrivilegeDocuments() const;

        // Creates the given user object in the given database.
        virtual Status insertPrivilegeDocument(const std::string& dbname,
                                               const BSONObj& userObj) const = 0;

        // Updates the given user object with the given update modifier.
        virtual Status updatePrivilegeDocument(const UserName& user,
                                               const BSONObj& updateObj) const = 0;

        // Removes users for the given database matching the given query.
        virtual Status removePrivilegeDocuments(const std::string& dbname,
                                                const BSONObj& query) const = 0;

        /**
         * Puts into the *dbnames vector the name of every database in the cluster.
         */
        virtual Status getAllDatabaseNames(std::vector<std::string>* dbnames) const = 0;

        /**
         * Puts into the *privDocs vector every privilege document from the given database's
         * system.users collection.
         */
        virtual Status getAllV1PrivilegeDocsForDB(const std::string& dbname,
                                                  std::vector<BSONObj>* privDocs) const = 0;

    protected:
        AuthzManagerExternalState(); // This class should never be instantiated directly.

        // Queries the userNamespace with the given query and returns the privilegeDocument found
        // in *result.  Returns Status::OK if it finds a document matching the query.  If it doesn't
        // find a document matching the query, returns a Status with code UserNotFound.  Other
        // errors may return other Status codes.
        virtual Status _findUser(const std::string& usersNamespace,
                                 const BSONObj& query,
                                 BSONObj* result) const = 0;

    };

} // namespace mongo
