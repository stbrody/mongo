/**
*    Copyright (C) 2013 10gen Inc.
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

#pragma once

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <string>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/role_graph.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/auth/user_name_hash.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/unordered_map.h"

namespace mongo {

    class PrivilegeDocumentParser;

    /**
     * Internal secret key info.
     */
    struct AuthInfo {
        User* user;
        BSONObj authParams;
    };
    extern AuthInfo internalSecurity; // set at startup and not changed after initialization.

    /**
     * Contains server/cluster-wide information about Authorization.
     */
    class AuthorizationManager {
        MONGO_DISALLOW_COPYING(AuthorizationManager);
    public:

        // The newly constructed AuthorizationManager takes ownership of "externalState"
        explicit AuthorizationManager(AuthzManagerExternalState* externalState);

        ~AuthorizationManager();

        static const std::string SERVER_RESOURCE_NAME;
        static const std::string CLUSTER_RESOURCE_NAME;
        static const std::string WILDCARD_RESOURCE_NAME;

        static const std::string USER_NAME_FIELD_NAME;
        static const std::string USER_SOURCE_FIELD_NAME;
        static const std::string PASSWORD_FIELD_NAME;
        static const std::string V1_USER_NAME_FIELD_NAME;
        static const std::string V1_USER_SOURCE_FIELD_NAME;

        // System roles for backwards compatibility with 2.2 and prior
        static const std::string SYSTEM_ROLE_V0_READ;
        static const std::string SYSTEM_ROLE_V0_READ_WRITE;
        static const std::string SYSTEM_ROLE_V0_ADMIN_READ;
        static const std::string SYSTEM_ROLE_V0_ADMIN_READ_WRITE;

        // TODO: Make the following functions no longer static.

        /**
         * Sets whether or not we allow old style (pre v2.4) privilege documents for this whole
         * server.
         */
        static void setSupportOldStylePrivilegeDocuments(bool enabled);

        /**
         * Returns true if we allow old style privilege privilege documents for this whole server.
         */
        static bool getSupportOldStylePrivilegeDocuments();

        /**
         * Sets whether or not access control enforcement is enabled for this whole server.
         */
        static void setAuthEnabled(bool enabled);

        /**
         * Returns true if access control is enabled on this server.
         */
        static bool isAuthEnabled();

        AuthzManagerExternalState* getExternalState() const;

        /**
         * Sets the version number of the authorization system.  Returns an invalid status if the
         * version number is not recognized.
         */
        Status setAuthorizationVersion(int version);

        /**
         * Returns the version number of the authorization system.
         */
        int getAuthorizationVersion();

        // Returns true if there exists at least one privilege document in the system.
        bool hasAnyPrivilegeDocuments() const;

        // Creates the given user object in the given database.
        Status insertPrivilegeDocument(const std::string& dbname, const BSONObj& userObj) const;

        // Updates the given user object with the given update modifier.
        Status updatePrivilegeDocument(const UserName& user, const BSONObj& updateObj) const;

        // Removes users for the given database matching the given query.
        // Writes into *numRemoved the number of user documents that were modified.
        Status removePrivilegeDocuments(const BSONObj& query, int* numRemoved) const;

        // Checks to see if "doc" is a valid privilege document, assuming it is stored in the
        // "system.users" collection of database "dbname".
        //
        // Returns Status::OK() if the document is good, or Status(ErrorCodes::BadValue), otherwise.
        Status checkValidPrivilegeDocument(const StringData& dbname, const BSONObj& doc);

        // Given a database name and a readOnly flag return an ActionSet describing all the actions
        // that an old-style user with those attributes should be given.
        ActionSet getActionsForOldStyleUser(const std::string& dbname, bool readOnly) const;

        // Returns an ActionSet of all actions that can be be granted to users.  This does not
        // include internal-only actions.
        ActionSet getAllUserActions();

        /**
         *  Returns the User object for the given userName in the out parameter "acquiredUser".
         *  If the user cache already has a user object for this user, it increments the refcount
         *  on that object and gives out a pointer to it.  If no user object for this user name
         *  exists yet in the cache, reads the user's privilege document from disk, builds up
         *  a User object, sets the refcount to 1, and gives that out.  The returned user may
         *  be invalid by the time the caller gets access to it.
         *  The AuthorizationManager retains ownership of the returned User object.
         *  On non-OK Status return values, acquiredUser will not be modified.
         */
        Status acquireUser(const UserName& userName, User** acquiredUser);

        /**
         * Decrements the refcount of the given User object.  If the refcount has gone to zero,
         * deletes the User.  Caller must stop using its pointer to "user" after calling this.
         */
        void releaseUser(User* user);

        /**
         * Marks the given user as invalid and removes it from the user cache.
         */
        void invalidateUser(User* user);

        /**
         * Marks the given user as invalid and removes it from the user cache.
         */
        void invalidateUserByName(const UserName& user);

        /**
         * Invalidates all users who's source is "dbname" and removes them from the user cache.
         */
        void invalidateUsersFromDB(const std::string& dbname);

        /**
         * Inserts the given user directly into the _userCache.  Used to add the internalSecurity
         * user into the cache at process startup.
         */
        void addInternalUser(User* user);

        /**
         * Returns true if the role name given refers to a valid system or user defined role.
         */
        bool roleExists(const RoleName& role);

        /**
         * Initializes the authorization manager.  Depending on what version the authorization
         * system is at, this may involve building up the user cache and/or the roles graph.
         * This function should be called once at startup and never again after that.
         */
        Status initialize();

        /**
         * Invalidates all of the contents of the user cache.
         */
        void invalidateUserCache();

        /**
         * Parses privDoc and fully initializes the user object (credentials, roles, and privileges)
         * with the information extracted from the privilege document.
         * This should never be called from outside the AuthorizationManager - the only reason it's
         * public instead of private is so it can be unit tested.
         */
        Status _initializeUserFromPrivilegeDocument(User* user, const BSONObj& privDoc);

        /**
         * Upgrades authorization data stored in collections from the v1 form (one system.users
         * collection per database) to the v2 form (a single admin.system.users collection).
         *
         * Returns Status::OK() if the AuthorizationManager and the admin.system.version collection
         * agree that the system is already upgraded, or if the upgrade completes successfully.
         *
         * This method will create and destroy an admin._newusers collection in addition to writing
         * to admin.system.users and admin.system.version.
         *
         * User information is taken from the in-memory user cache, constructed at start-up.  This
         * is safe to do because MongoD and MongoS build complete copies of the data stored in
         * *.system.users at start-up if they detect that the upgrade has not yet completed.
         */
        Status upgradeAuthCollections();

    private:

        /**
         * Returns the current version number of the authorization system.  Should only be called
         * when holding _lock.
         */
        int _getVersion_inlock() const { return _version; }

        /**
         * Modifies the given User object by inspecting its roles and giving it the relevant
         * privileges from those roles.
         */
        void _initializeUserPrivilegesFromRoles_inlock(User* user);

        /**
         * Invalidates all User objects in the cache and removes them from the cache.
         * Should only be called when already holding _lock.
         * TODO(spencer): This only exists because we're currently calling initializeAllV1UserData
         * every time user data is changed.  Once we only call that once at startup, this function
         * should be removed.
         */
        void _invalidateUserCache_inlock();

        /**
         * Initializes the user cache with User objects for every v0 and v1 user document in the
         * system, by reading the system.users collection of every database.  If this function
         * returns a non-ok Status, the _userCache should be considered corrupt and must be
         * discarded.  This function should be called once at startup (only if the system hasn't yet
         * been upgraded to V2 user data format) and never again after that.
         */
        Status _initializeAllV1UserData();


        static bool _doesSupportOldStylePrivileges;

        // True if access control enforcement is enabled on this node (ie it was started with
        // --auth or --keyFile).
        // This is a config setting, set at startup and not changing after initialization.
        static bool _authEnabled;

        // Integer that represents what format version the privilege documents in the system are.
        // The current version is 2.  When upgrading to v2.6 or later from v2.4 or prior, the
        // version is 1.  After running the upgrade process to upgrade to the new privilege document
        // format, the version will be 2.
        // All reads/writes to _version must be done within _lock.
        int _version;

        /**
         * Used for parsing privilege documents.  Set whenever _version is set.  Guarded by _lock.
         */
        scoped_ptr<PrivilegeDocumentParser> _parser;

        scoped_ptr<AuthzManagerExternalState> _externalState;

        /**
         * Caches User objects with information about user privileges, to avoid the need to
         * go to disk to read user privilege documents whenever possible.  Every User object
         * has a reference count - the AuthorizationManager must not delete a User object in the
         * cache unless its reference count is zero.
         */
        unordered_map<UserName, User*> _userCache;

        /**
         * Stores a full representation of all roles in the system (both user-defined and built-in)
         */
        RoleGraph _roleGraph;

        /**
         * Protects _userCache, _roleGraph, _version, and _parser.
         */
        boost::mutex _lock;
    };

} // namespace mongo
