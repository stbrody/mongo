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

#include "mongo/platform/basic.h"

#include "mongo/db/auth/authorization_manager.h"

#include <boost/thread/mutex.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/mutable/element.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/authz_documents_update_guard.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/role_graph.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_document_parser.h"
#include "mongo/db/auth/user_management_commands_parser.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/auth/user_name_hash.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/ops/update_driver.h"
#include "mongo/platform/compiler.h"
#include "mongo/platform/unordered_map.h"
#include "mongo/util/log.h"
#include "mongo/util/map_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    AuthInfo internalSecurity;

    MONGO_INITIALIZER(SetupInternalSecurityUser)(InitializerContext* context) {
        User* user = new User(UserName("__system", "local"));

        user->incrementRefCount(); // Pin this user so the ref count never drops below 1.
        ActionSet allActions;
        allActions.addAllActions();
        PrivilegeVector privileges;
        RoleGraph::generateUniversalPrivileges(&privileges);
        user->addPrivileges(privileges);
        internalSecurity.user = user;

        return Status::OK();
    }

    const std::string AuthorizationManager::SERVER_RESOURCE_NAME = "$SERVER";
    const std::string AuthorizationManager::CLUSTER_RESOURCE_NAME = "$CLUSTER";
    const std::string AuthorizationManager::WILDCARD_RESOURCE_NAME = "*";
    const std::string AuthorizationManager::USER_NAME_FIELD_NAME = "name";
    const std::string AuthorizationManager::USER_SOURCE_FIELD_NAME = "source";
    const std::string AuthorizationManager::ROLE_NAME_FIELD_NAME = "name";
    const std::string AuthorizationManager::ROLE_SOURCE_FIELD_NAME = "source";
    const std::string AuthorizationManager::PASSWORD_FIELD_NAME = "pwd";
    const std::string AuthorizationManager::V1_USER_NAME_FIELD_NAME = "user";
    const std::string AuthorizationManager::V1_USER_SOURCE_FIELD_NAME = "userSource";

    bool AuthorizationManager::_doesSupportOldStylePrivileges = true;
    bool AuthorizationManager::_authEnabled = false;


    AuthorizationManager::AuthorizationManager(AuthzManagerExternalState* externalState) :
        _externalState(externalState),
        _roleGraphState(roleGraphStateInitial) {

        setAuthorizationVersion(2);
    }

    AuthorizationManager::~AuthorizationManager() {
        for (unordered_map<UserName, User*>::iterator it = _userCache.begin();
                it != _userCache.end(); ++it) {
            if (it->second != internalSecurity.user) {
                // The internal user should never be deleted.
                delete it->second ;
            }
        }
    }

    AuthzManagerExternalState* AuthorizationManager::getExternalState() const {
        return _externalState.get();
    }

    Status AuthorizationManager::setAuthorizationVersion(int version) {
        boost::lock_guard<boost::mutex> lk(_lock);

        if (version == 1) {
            _parser.reset(new V1UserDocumentParser());
        } else if (version == 2) {
            _parser.reset(new V2UserDocumentParser());
        } else {
            return Status(ErrorCodes::UnsupportedFormat,
                          mongoutils::str::stream() <<
                                  "Unrecognized authorization format version: " <<
                                  version);
        }

        _version = version;
        return Status::OK();
    }

    int AuthorizationManager::getAuthorizationVersion() {
        boost::lock_guard<boost::mutex> lk(_lock);
        return _getVersion_inlock();
    }

    void AuthorizationManager::setSupportOldStylePrivilegeDocuments(bool enabled) {
        _doesSupportOldStylePrivileges = enabled;
    }

    bool AuthorizationManager::getSupportOldStylePrivilegeDocuments() {
        return _doesSupportOldStylePrivileges;
    }

    void AuthorizationManager::setAuthEnabled(bool enabled) {
        _authEnabled = enabled;
    }

    bool AuthorizationManager::isAuthEnabled() {
        return _authEnabled;
    }

    bool AuthorizationManager::hasAnyPrivilegeDocuments() const {
        return _externalState->hasAnyPrivilegeDocuments();
    }

    Status AuthorizationManager::insertPrivilegeDocument(const std::string& dbname,
                                                         const BSONObj& userObj,
                                                         const BSONObj& writeConcern) const {
        return _externalState->insertPrivilegeDocument(dbname, userObj, writeConcern);
    }

    Status AuthorizationManager::updatePrivilegeDocument(const UserName& user,
                                                         const BSONObj& updateObj,
                                                         const BSONObj& writeConcern) const {
        return _externalState->updatePrivilegeDocument(user, updateObj, writeConcern);
    }

    Status AuthorizationManager::removePrivilegeDocuments(const BSONObj& query,
                                                          const BSONObj& writeConcern,
                                                          int* numRemoved) const {
        return _externalState->removePrivilegeDocuments(query, writeConcern, numRemoved);
    }

    Status AuthorizationManager::insertRoleDocument(const BSONObj& roleObj,
                                                    const BSONObj& writeConcern) const {
        Status status = _externalState->insert(NamespaceString("admin.system.roles"),
                                               roleObj,
                                               writeConcern);
        if (status.isOK()) {
            return status;
        }
        if (status.code() == ErrorCodes::DuplicateKey) {
            std::string name = roleObj[AuthorizationManager::ROLE_NAME_FIELD_NAME].String();
            std::string source = roleObj[AuthorizationManager::ROLE_SOURCE_FIELD_NAME].String();
            return Status(ErrorCodes::DuplicateKey,
                          mongoutils::str::stream() << "Role \"" << name << "@" << source <<
                                  "\" already exists");
        }
        if (status.code() == ErrorCodes::UnknownError) {
            return Status(ErrorCodes::RoleModificationFailed, status.reason());
        }
        return status;
    }

    Status AuthorizationManager::queryAuthzDocument(
            const NamespaceString& collectionName,
            const BSONObj& query,
            const boost::function<void(const BSONObj&)>& resultProcessor) {
        return _externalState->query(collectionName, query, resultProcessor);
    }

    bool AuthorizationManager::roleExists(const RoleName& role) {
        boost::lock_guard<boost::mutex> lk(_lock);
        return _roleGraph.roleExists(role);
    }

    Status AuthorizationManager::getBSONForRole(RoleGraph* graph,
                                                const RoleName& roleName,
                                                mutablebson::Element result) {
        if (!graph->roleExists(roleName)) {
            return Status(ErrorCodes::RoleNotFound,
                          mongoutils::str::stream() << roleName.getFullName() <<
                                  "does not name an existing role");
        }
        std::string id = mongoutils::str::stream() << roleName.getDB() << "." << roleName.getRole();
        result.appendString("_id", id);
        result.appendString("name", roleName.getRole());
        result.appendString("source", roleName.getDB());

        // Build privileges array
        mutablebson::Element privilegesArrayElement =
                result.getDocument().makeElementArray("privileges");
        result.pushBack(privilegesArrayElement);
        const PrivilegeVector& privileges = graph->getDirectPrivileges(roleName);
        for (PrivilegeVector::const_iterator it = privileges.begin();
                it != privileges.end(); ++it) {
            std::string errmsg;
            ParsedPrivilege privilege;
            if (!ParsedPrivilege::privilegeToParsedPrivilege(*it, &privilege, &errmsg)) {
                return Status(ErrorCodes::BadValue, errmsg);
            }
            privilegesArrayElement.appendObject("privileges", privilege.toBSON());
        }

        // Build roles array
        mutablebson::Element rolesArrayElement = result.getDocument().makeElementArray("roles");
        result.pushBack(rolesArrayElement);
        RoleNameIterator nameIt = graph->getDirectSubordinates(roleName);
        while (nameIt.more()) {
            const RoleName& subRole = nameIt.next();
            mutablebson::Element roleObj = result.getDocument().makeElementObject("");
            roleObj.appendString("name", subRole.getRole());
            roleObj.appendString("source", subRole.getDB());
            rolesArrayElement.pushBack(roleObj);
        }

        return Status::OK();
    }

    void AuthorizationManager::_initializeUserPrivilegesFromRoles_inlock(User* user) {
        const User::RoleDataMap& roles = user->getRoles();
        for (User::RoleDataMap::const_iterator it = roles.begin(); it != roles.end(); ++it) {
            const User::RoleData& role= it->second;
            if (role.hasRole)
                user->addPrivileges(_roleGraph.getAllPrivileges(role.name));
        }
    }

    Status AuthorizationManager::_initializeUserFromPrivilegeDocument(
            User* user, const BSONObj& privDoc) {
        std::string userName = _parser->extractUserNameFromUserDocument(privDoc);
        if (userName != user->getName().getUser()) {
            return Status(ErrorCodes::BadValue,
                          mongoutils::str::stream() << "User name from privilege document \""
                                  << userName
                                  << "\" doesn't match name of provided User \""
                                  << user->getName().getUser()
                                  << "\"",
                          0);
        }

        Status status = _parser->initializeUserCredentialsFromUserDocument(user, privDoc);
        if (!status.isOK()) {
            return status;
        }
        status = _parser->initializeUserRolesFromUserDocument(user,
                                                                 privDoc,
                                                                 user->getName().getDB());
        if (!status.isOK()) {
            return status;
        }
        _initializeUserPrivilegesFromRoles_inlock(user);
        return Status::OK();
    }

    Status AuthorizationManager::acquireUser(const UserName& userName, User** acquiredUser) {
        boost::lock_guard<boost::mutex> lk(_lock);
        return _acquireUser_inlock(userName, acquiredUser);
    }

    Status AuthorizationManager::_acquireUser_inlock(const UserName& userName,
                                                     User** acquiredUser) {
        unordered_map<UserName, User*>::iterator it = _userCache.find(userName);
        if (it != _userCache.end()) {
            fassert(16914, it->second);
            fassert(17003, it->second->isValid());
            fassert(17008, it->second->getRefCount() > 0);
            it->second->incrementRefCount();
            *acquiredUser = it->second;
            return Status::OK();
        }

        // Put the new user into an auto_ptr temporarily in case there's an error while
        // initializing the user.
        auto_ptr<User> userHolder(new User(userName));
        User* user = userHolder.get();

        BSONObj userObj;
        Status status = _externalState->getPrivilegeDocument(userName,
                                                             _getVersion_inlock(),
                                                             &userObj);
        if (!status.isOK()) {
            return status;
        }

        status = _initializeUserFromPrivilegeDocument(user, userObj);
        if (!status.isOK()) {
            return status;
        }

        user->incrementRefCount();
        _userCache.insert(make_pair(userName, userHolder.release()));
        *acquiredUser = user;
        return Status::OK();
    }

    void AuthorizationManager::releaseUser(User* user) {
        if (user == internalSecurity.user) {
            return;
        }

        boost::lock_guard<boost::mutex> lk(_lock);
        user->decrementRefCount();
        if (user->getRefCount() == 0) {
            // If it's been invalidated then it's not in the _userCache anymore.
            if (user->isValid()) {
                MONGO_COMPILER_VARIABLE_UNUSED bool erased = _userCache.erase(user->getName());
                dassert(erased);
            }
            delete user;
        }
    }

    void AuthorizationManager::invalidateUser(User* user) {
        boost::lock_guard<boost::mutex> lk(_lock);
        if (!user->isValid()) {
            return;
        }

        unordered_map<UserName, User*>::iterator it = _userCache.find(user->getName());
        massert(17052,
                mongoutils::str::stream() <<
                        "Invalidating cache for user " << user->getName().getFullName() <<
                        " failed as it is not present in the user cache",
                it != _userCache.end() && it->second == user);
        _userCache.erase(it);
        user->invalidate();
    }

    void AuthorizationManager::invalidateUserByName(const UserName& userName) {
        boost::lock_guard<boost::mutex> lk(_lock);

        unordered_map<UserName, User*>::iterator it = _userCache.find(userName);
        if (it == _userCache.end()) {
            return;
        }

        User* user = it->second;
        _userCache.erase(it);
        user->invalidate();
    }

    void AuthorizationManager::invalidateUsersFromDB(const std::string& dbname) {
        boost::lock_guard<boost::mutex> lk(_lock);

        unordered_map<UserName, User*>::iterator it = _userCache.begin();
        while (it != _userCache.end()) {
            User* user = it->second;
            if (user->getName().getDB() == dbname) {
                _userCache.erase(it++);
                user->invalidate();
            } else {
                ++it;
            }
        }
    }


    void AuthorizationManager::addInternalUser(User* user) {
        boost::lock_guard<boost::mutex> lk(_lock);
        _userCache.insert(make_pair(user->getName(), user));
    }

    void AuthorizationManager::invalidateUserCache() {
        boost::lock_guard<boost::mutex> lk(_lock);
        _invalidateUserCache_inlock();
    }

    void AuthorizationManager::_invalidateUserCache_inlock() {
        for (unordered_map<UserName, User*>::iterator it = _userCache.begin();
                it != _userCache.end(); ++it) {
            if (it->second->getName() == internalSecurity.user->getName()) {
                // Don't invalidate the internal user
                continue;
            }
            it->second->invalidate();
            // Need to decrement ref count and manually clean up User object to prevent memory leaks
            // since we're pinning all User objects by incrementing their ref count when we
            // initially populate the cache.
            // TODO(spencer): remove this once we're not pinning User objects.
            it->second->decrementRefCount();
            if (it->second->getRefCount() == 0)
                delete it->second;
        }
        _userCache.clear();
        // Make sure the internal user stays in the cache.
        _userCache.insert(make_pair(internalSecurity.user->getName(), internalSecurity.user));
    }

    Status AuthorizationManager::initialize() {
        Status status = initializeRoleGraph();
        if (!status.isOK()) {
            if (status == ErrorCodes::GraphContainsCycle) {
                error() << "Cycle detected in admin.system.roles; role inheritance disabled. "
                    "TODO EXPLAIN TO REMEDY. " << status.reason();
            }
            else {
                error() << "Could not generate role graph from admin.system.roles; "
                    "only system roles available. TODO EXPLAIN REMEDY. " << status;
            }
        }

        if (getAuthorizationVersion() < 2) {
            // If we are not yet upgraded to the V2 authorization format, build up a read-only
            // view of the V1 style authorization data.
            return _initializeAllV1UserData();
        }

        return Status::OK();
    }

    Status AuthorizationManager::_initializeAllV1UserData() {
        boost::lock_guard<boost::mutex> lk(_lock);
        _invalidateUserCache_inlock();
        V1UserDocumentParser parser;

        try {
            std::vector<std::string> dbNames;
            Status status = _externalState->getAllDatabaseNames(&dbNames);
            if (!status.isOK()) {
                return status;
            }

            for (std::vector<std::string>::iterator dbIt = dbNames.begin();
                    dbIt != dbNames.end(); ++dbIt) {
                std::string dbname = *dbIt;
                std::vector<BSONObj> privDocs;
                Status status = _externalState->getAllV1PrivilegeDocsForDB(dbname, &privDocs);
                if (!status.isOK()) {
                    return status;
                }

                for (std::vector<BSONObj>::iterator docIt = privDocs.begin();
                        docIt != privDocs.end(); ++docIt) {
                    const BSONObj& privDoc = *docIt;

                    std::string source;
                    if (privDoc.hasField("userSource")) {
                        source = privDoc["userSource"].String();
                    } else {
                        source = dbname;
                    }
                    UserName userName(privDoc["user"].String(), source);
                    if (userName == internalSecurity.user->getName()) {
                        // Don't let clients override the internal user by creating a user with the
                        // same name.
                        continue;
                    }

                    User* user = mapFindWithDefault(_userCache, userName, static_cast<User*>(NULL));
                    if (!user) {
                        user = new User(userName);
                        // Make sure the user always has a refCount of at least 1 so it's
                        // effectively "pinned" and will never be removed from the _userCache
                        // unless the whole cache is invalidated.
                        user->incrementRefCount();
                        _userCache.insert(make_pair(userName, user));
                    }

                    if (source == dbname || source == "$external") {
                        status = parser.initializeUserCredentialsFromUserDocument(user,
                                                                                  privDoc);
                        if (!status.isOK()) {
                            return status;
                        }
                    }
                    status = parser.initializeUserRolesFromUserDocument(user, privDoc, dbname);
                    if (!status.isOK()) {
                        return status;
                    }
                    _initializeUserPrivilegesFromRoles_inlock(user);
                }
            }
        } catch (const DBException& e) {
            return e.toStatus();
        } catch (const std::exception& e) {
            return Status(ErrorCodes::InternalError, e.what());
        }

        return Status::OK();
    }

    bool AuthorizationManager::tryAcquireAuthzUpdateLock(const StringData& why) {
        return _externalState->tryAcquireAuthzUpdateLock(why);
    }

    void AuthorizationManager::releaseAuthzUpdateLock() {
        return _externalState->releaseAuthzUpdateLock();
    }

    namespace {
        BSONObj userAsV2PrivilegeDocument(const User& user) {
            BSONObjBuilder builder;

            const UserName& name = user.getName();
            builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, name.getUser());
            builder.append(AuthorizationManager::USER_SOURCE_FIELD_NAME, name.getDB());

            const User::CredentialData& credentials = user.getCredentials();
            if (!credentials.isExternal) {
                BSONObjBuilder credentialsBuilder(builder.subobjStart("credentials"));
                credentialsBuilder.append("MONGODB-CR", credentials.password);
                credentialsBuilder.doneFast();
            }

            BSONArrayBuilder rolesArray(builder.subarrayStart("roles"));
            const User::RoleDataMap& roles = user.getRoles();
            for (User::RoleDataMap::const_iterator it = roles.begin(); it != roles.end(); ++it) {
                const User::RoleData& role = it->second;
                BSONObjBuilder roleBuilder(rolesArray.subobjStart());
                roleBuilder.append("name", role.name.getRole());
                roleBuilder.append("source", role.name.getDB());
                roleBuilder.appendBool("canDelegate", role.canDelegate);
                roleBuilder.appendBool("hasRole", role.hasRole);
                roleBuilder.doneFast();
            }
            rolesArray.doneFast();
            return builder.obj();
        }

        const NamespaceString adminCommandNamespace("admin.$cmd");
        const NamespaceString rolesCollectionName("admin.system.roles");
        const NamespaceString newusersCollectionName("admin._newusers");
        const NamespaceString usersCollectionName("admin.system.users");
        const NamespaceString backupUsersCollectionName("admin.backup.users");
        const NamespaceString versionCollectionName("admin.system.version");
        const BSONObj versionDocumentQuery = BSON("_id" << 1);

        /**
         * Fetches the admin.system.version document and extracts the currentVersion field's
         * value, supposing it is an integer, and writes it to outVersion.
         */
        Status readAuthzVersion(AuthzManagerExternalState* externalState, int* outVersion) {
            BSONObj versionDoc;
            Status status = externalState->findOne(
                    versionCollectionName, versionDocumentQuery, &versionDoc);
            if (!status.isOK() && ErrorCodes::NoMatchingDocument != status) {
                return status;
            }
            BSONElement currentVersionElement = versionDoc["currentVersion"];
            if (!versionDoc.isEmpty() && !currentVersionElement.isNumber()) {
                return Status(ErrorCodes::TypeMismatch,
                              "Field 'currentVersion' in admin.system.version must be a number.");
            }
            *outVersion = currentVersionElement.numberInt();
            return Status::OK();
        }
    }  // namespace

    Status AuthorizationManager::upgradeAuthCollections() {
        AuthzDocumentsUpdateGuard lkUpgrade(this);
        if (!lkUpgrade.tryLock("Upgrade authorization data")) {
            return Status(ErrorCodes::LockBusy, "Could not lock auth data upgrade process lock.");
        }
        boost::lock_guard<boost::mutex> lkLocal(_lock);
        int durableVersion = 0;
        Status status = readAuthzVersion(_externalState.get(), &durableVersion);
        if (!status.isOK())
            return status;

        if (_version == 2) {
            switch (durableVersion) {
            case 0:
            case 1: {
                const char msg[] = "User data format version in memory and on disk inconsistent; "
                    "please restart this node.";
                error() << msg;
                return Status(ErrorCodes::UserDataInconsistent, msg);
            }
            case 2:
                return Status::OK();
            default:
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() <<
                              "Cannot upgrade admin.system.version to 2 from " <<
                              durableVersion);
            }
        }
        fassert(17113, _version == 1);
        switch (durableVersion) {
        case 0:
        case 1:
            break;
        case 2: {
            const char msg[] = "User data format version in memory and on disk inconsistent; "
                "please restart this node.";
            error() << msg;
            return Status(ErrorCodes::UserDataInconsistent, msg);
        }
        default:
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() <<
                              "Cannot upgrade admin.system.version from 2 to " <<
                              durableVersion);
        }

        BSONObj writeConcern;
        // Upgrade from v1 to v2.
        status = _externalState->copyCollection(usersCollectionName,
                                                backupUsersCollectionName,
                                                writeConcern);
        if (!status.isOK())
            return status;
        status = _externalState->dropCollection(newusersCollectionName, writeConcern);
        if (!status.isOK())
            return status;
        status = _externalState->createIndex(
                newusersCollectionName,
                BSON(USER_NAME_FIELD_NAME << 1 << USER_SOURCE_FIELD_NAME << 1),
                true, // unique
                writeConcern
                );
        if (!status.isOK())
            return status;

        for (unordered_map<UserName, User*>::const_iterator iter = _userCache.begin();
             iter != _userCache.end(); ++iter) {

            // Do not create a user document for the internal user.
            if (iter->second == internalSecurity.user)
                continue;

            status = _externalState->insert(
                    newusersCollectionName, userAsV2PrivilegeDocument(*iter->second),
                    writeConcern);
            if (!status.isOK())
                return status;
        }
        status = _externalState->renameCollection(newusersCollectionName,
                                                  usersCollectionName,
                                                  writeConcern);
        if (!status.isOK())
            return status;
        status = _externalState->updateOne(
                versionCollectionName,
                versionDocumentQuery,
                BSON("$set" << BSON("currentVersion" << 2)),
                true,
                writeConcern);
        if (!status.isOK())
            return status;
        _version = 2;
        return status;
    }

    namespace {
        const std::string ROLE_NAME_FIELD_NAME("name");
        const std::string ROLE_SOURCE_FIELD_NAME("source");
        const std::string ROLE_PRIVILEGES_FIELD_NAME("privileges");
        const std::string ROLE_ROLES_FIELD_NAME("roles");
        const std::string ROLE_CAN_DELEGATE_FIELD_NAME("canDelegate");
        const std::string ROLE_HAS_ROLE_FIELD_NAME("hasRole");

        /**
         * Structure representing information parsed out of a role document.
         */
        struct RoleInfo {
            RoleName name;
            std::vector<RoleName> roles;
            PrivilegeVector privileges;
        };

        /**
         * Parses the role name out of a BSON document.
         */
        Status parseRoleNameFromDocument(const BSONObj& doc, RoleName* name) {
            BSONElement nameElement;
            BSONElement sourceElement;
            Status status = bsonExtractTypedField(doc, ROLE_NAME_FIELD_NAME, String, &nameElement);
            if (!status.isOK())
                return status;
            status = bsonExtractTypedField(doc, ROLE_SOURCE_FIELD_NAME, String, &sourceElement);
            if (!status.isOK())
                return status;
            *name = RoleName(StringData(nameElement.valuestr(), nameElement.valuestrsize() - 1),
                             StringData(sourceElement.valuestr(), sourceElement.valuestrsize() - 1));
            return status;
        }

        /**
         * Checks whether the given "roleName" corresponds with the given _id field.
         * In admin.system.roles, documents with role name "role@db" must have _id
         * "db.role".
         *
         * Returns Status::OK if the two values are compatible.
         */
        Status checkIdMatchesRoleName(const BSONElement& idElement, const RoleName& roleName) {
            if (idElement.type() != String) {
                return Status(ErrorCodes::TypeMismatch,
                              "Role document _id fields must be strings.");
            }
            StringData idField(idElement.valuestr(), idElement.valuestrsize() - 1);
            size_t firstDot = idField.find('.');
            if (firstDot == std::string::npos ||
                idField.substr(0, firstDot) !=  roleName.getDB() ||
                idField.substr(firstDot + 1) != roleName.getRole()) {
                return Status(ErrorCodes::FailedToParse, mongoutils::str::stream() <<
                              "Role document _id fields must be encoded as the string "
                              "dbname.rolename.  Found " << idField << " for " <<
                              roleName.getFullName());
            }
            return Status::OK();
        }

        /**
         * Parses "idElement" to extract the role name, according to the "dbname.role" convention
         * used for admin.system.roles documents.
         */
        Status getRoleNameFromIdField(const BSONElement& idElement, RoleName* roleName) {
            if (idElement.type() != String) {
                return Status(ErrorCodes::TypeMismatch,
                              "Role document _id fields must be strings.");
            }
            StringData idField(idElement.valuestr(), idElement.valuestrsize() - 1);
            size_t dotPos = idField.find('.');
            if (dotPos == std::string::npos) {
                return Status(ErrorCodes::BadValue,
                              "Role document _id fields must have the form dbname.rolename");
            }
            *roleName = RoleName(idField.substr(dotPos + 1), idField.substr(0, dotPos));
            return Status::OK();
        }

        /**
         * Parses information about a role from a BSON document.
         */
        Status parseRoleFromDocument(const BSONObj& doc, RoleInfo* role) {
            BSONElement privilegesElement;
            BSONElement rolesElement;
            Status status = parseRoleNameFromDocument(doc, &role->name);
            if (!status.isOK())
                return status;
            status = checkIdMatchesRoleName(doc["_id"], role->name);
            if (!status.isOK())
                return status;
            status = bsonExtractTypedField(
                    doc, ROLE_PRIVILEGES_FIELD_NAME, Array, &privilegesElement);
            if (!status.isOK())
                return status;
            status = bsonExtractTypedField(doc, ROLE_ROLES_FIELD_NAME, Array, &rolesElement);
            if (!status.isOK())
                return status;
            BSONForEach(singleRoleElement, rolesElement.Obj()) {
                if (singleRoleElement.type() != Object) {
                    return Status(ErrorCodes::TypeMismatch,
                                  "Elements of roles array must be objects.");
                }
                RoleName possessedRoleName;
                status = parseRoleNameFromDocument(singleRoleElement.Obj(), &possessedRoleName);
                if (!status.isOK())
                    return status;
                role->roles.push_back(possessedRoleName);
            }

            // TODO: Parse privileges from "doc" and add to role->privileges.

            return Status::OK();
        }

        /**
         * Updates "roleGraph" by adding the role described by "role".  If the
         * name "role" already exists, it is replaced.  Any subordinate roles
         * mentioned in role.roles are created, if needed, with empty
         * privilege and subordinate role lists.
         *
         * Should _only_ fail if the role to replace is a builtin role, in which
         * case it will return ErrorCodes::InvalidRoleModification.
         */
        Status replaceRole(RoleGraph* roleGraph, const RoleInfo& role) {
            Status status = roleGraph->removeAllPrivilegesFromRole(role.name);
            if (status == ErrorCodes::RoleNotFound) {
                fassert(17152, roleGraph->createRole(role.name).isOK());
            }
            else if (!status.isOK()) {
                return status;
            }
            fassert(17153, roleGraph->removeAllRolesFromRole(role.name).isOK());
            for (size_t i = 0; i < role.roles.size(); ++i) {
                const RoleName& grantedRole = role.roles[i];
                status = roleGraph->createRole(grantedRole);
                fassert(17154, status.isOK() || status == ErrorCodes::DuplicateKey);
                fassert(17155, roleGraph->addRoleToRole(role.name, grantedRole).isOK());
            }
            fassert(17156, roleGraph->addPrivilegesToRole(role.name, role.privileges).isOK());
            return Status::OK();
        }

        /**
         * Adds the role described in "doc" to "roleGraph".
         *
         * Returns a status.
         */
        Status addRoleFromDocument(RoleGraph* roleGraph, const BSONObj& doc) {
            RoleInfo role;
            Status status = parseRoleFromDocument(doc, &role);
            if (!status.isOK())
                return status;
            status = replaceRole(roleGraph, role);
            return status;
        }

        /**
         * Adds the role described in "doc" to "roleGraph".  If the role cannot be added, due to
         * some error in "doc"< logs a warning.
         */
        void addRoleFromDocumentOrWarn(RoleGraph* roleGraph, const BSONObj& doc) {
            Status status = addRoleFromDocument(roleGraph, doc);
            if (!status.isOK()) {
                warning() << "Skipping invalid role document.  " << status << "; document " << doc;
            }
        }

        /**
         * Updates roleGraph for an insert-type oplog operation on admin.system.roles.
         */
        Status handleOplogInsert(RoleGraph* roleGraph, const BSONObj& insertedObj) {
            RoleInfo role;
            Status status = parseRoleFromDocument(insertedObj, &role);
            if (!status.isOK())
                return status;
            status = replaceRole(roleGraph, role);
            return status;
        }

        /**
         * Updates roleGraph for an update-type oplog operation on admin.system.roles.
         *
         * Treats all updates as upserts.
         */
        Status handleOplogUpdate(RoleGraph* roleGraph,
                                 const BSONObj& updatePattern,
                                 const BSONObj& queryPattern) {
            RoleName roleToUpdate;
            Status status = getRoleNameFromIdField(queryPattern["_id"], &roleToUpdate);
            if (!status.isOK())
                return status;

            UpdateDriver::Options updateOptions;
            updateOptions.upsert = true;
            UpdateDriver driver(updateOptions);
            status = driver.parse(updatePattern);
            if (!status.isOK())
                return status;

            mutablebson::Document roleDocument;
            status = AuthorizationManager::getBSONForRole(
                    roleGraph, roleToUpdate, roleDocument.root());
            if (status == ErrorCodes::RoleNotFound) {
                roleDocument.root().appendElement(queryPattern["_id"]);
                status = driver.createFromQuery(queryPattern, roleDocument);
            }
            if (!status.isOK())
                return status;

            status = driver.update(StringData(), &roleDocument, NULL);
            if (!status.isOK())
                return status;

            // Now use the updated document to totally replace the role in the graph!
            RoleInfo role;
            status = parseRoleFromDocument(roleDocument.getObject(), &role);
            if (!status.isOK())
                return status;
            status = replaceRole(roleGraph, role);

            return status;
        }

        /**
         * Updates roleGraph for a delete-type oplog operation on admin.system.roles.
         */
        Status handleOplogDelete(
                RoleGraph* roleGraph,
                const BSONObj& deletePattern) {

            RoleName roleToDelete;
            Status status = getRoleNameFromIdField(deletePattern["_id"], &roleToDelete);
            if (!status.isOK())
                return status;
            status = roleGraph->deleteRole(roleToDelete);
            if (ErrorCodes::RoleNotFound == status) {
                // Double-delete can happen in oplog application.
                status = Status::OK();
            }
            return status;
        }

        /**
         * Updates roleGraph for command-type oplog operations on the admin database.
         */
        Status handleOplogCommand(RoleGraph* roleGraph, const BSONObj& cmdObj) {
            const StringData cmdName(cmdObj.firstElement().fieldNameStringData());
            if (cmdName == "drop") {
                if (cmdObj.firstElement().str() == rolesCollectionName.coll()) {
                    *roleGraph = RoleGraph();
                }
                return Status::OK();
            }
            if (cmdName == "dropDatabase") {
                *roleGraph = RoleGraph();
                return Status::OK();
            }
            if (cmdName == "renameCollection") {
                if (cmdObj.firstElement().str() == rolesCollectionName.ns()) {
                    *roleGraph = RoleGraph();
                    return Status::OK();
                }
                if (cmdObj["to"].str() == rolesCollectionName.ns()) {
                    *roleGraph = RoleGraph();
                    return Status(ErrorCodes::OplogOperationUnsupported,
                                  "Renaming into admin.system.roles produces inconsistent state; "
                                  "must resynchronize role graph.");
                }
                return Status::OK();
            }
            if (cmdName == "dropIndexes") {
                return Status::OK();
            }
            if ((cmdName == "collMod" || cmdName == "emptyCappedCollection") &&
                cmdObj.firstElement().str() != rolesCollectionName.coll()) {

                // We don't care about these if they're not on the roles collection.
                return Status::OK();
            }
            //  No other commands expected.  Warn.
            return Status(ErrorCodes::OplogOperationUnsupported, "Unsupported oplog operation");
        }

        /**
         * Updates roleGraph for a given set of logOp() parameters.
         *
         * Returns a status indicating success or failure.
         */
        Status updateRoleGraphWithLogOpSignature(
                RoleGraph* roleGraph,
                const char* op,
                const NamespaceString& ns,
                const BSONObj& o,
                BSONObj* o2) {

            if (op == StringData("db", StringData::LiteralTag()))
                return Status::OK();
            if (op[0] == '\0' || op[1] != '\0') {
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() << "Unrecognized \"op\" field value \"" <<
                              op << '"');
            }

            if (ns.db() != rolesCollectionName.db())
                return Status::OK();

            if (ns.isCommand()) {
                if (*op == 'c') {
                    return handleOplogCommand(roleGraph, o);
                }
                else {
                    return Status(ErrorCodes::BadValue,
                                  "Non-command oplog entry on admin.$cmd namespace");
                }
            }

            if (ns.coll() != rolesCollectionName.coll())
                return Status::OK();

            switch (*op) {
            case 'i':
                return handleOplogInsert(roleGraph, o);
            case 'u':
                if (!o2) {
                    return Status(ErrorCodes::InternalError,
                                  "Missing query pattern in update oplog entry.");
                }
                return handleOplogUpdate(roleGraph, o, *o2);
            case 'd':
                return handleOplogDelete(roleGraph, o);
            case 'n':
                return Status::OK();
            case 'c':
                return Status(ErrorCodes::BadValue,
                              "Namespace admin.system.roles is not a valid target for commands");
            default:
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() << "Unrecognized \"op\" field value \"" <<
                              op << '"');
            }
        }

        /**
         * Updates roleGraph from a given oplog document targeted at the "admin" database.
         *
         * Returns a status indicating success.  On failures other than OplogOperationUnsupported,
         * roleGraph is in an unchanged state.  For OplogOperationUnsupported, that may be true,but
         * roleGraph should be considered invalid.
         */
        Status updateRoleGraphFromOplog(RoleGraph* roleGraph, const BSONObj& oplogEntry) {
            const char* const op = oplogEntry.getStringField("op");
            const NamespaceString ns(oplogEntry.getStringField("ns"));
            BSONObj o = oplogEntry.getObjectField("o");
            BSONObj o2 = oplogEntry.getObjectField("o2");
            return updateRoleGraphWithLogOpSignature(roleGraph, op, ns, o, &o2);
        }

        /**
         * Updates roleGraph from a given oplog document targeted at the "admin" database.
         *
         * On OplogOperationUnsupported, throws a user exception.  This is done because the only
         * multi-document query method available in mongos and mongod takes a function that returns
         * void as its argument, and this error must be treated specially in initializeRoleGraph().

         * Logs a warning for any other failure, making no change to the role graph.
         */
        void updateRoleGraphFromOplogOrWarn(RoleGraph* roleGraph, const BSONObj& oplogEntry) {
            Status status = updateRoleGraphFromOplog(roleGraph, oplogEntry);
            if (!status.isOK()) {
                if (status == ErrorCodes::OplogOperationUnsupported) {
                    uasserted(status.code(), status.reason());
                }
                warning() << "Could not apply oplog entry to role graph (" << status << "): " <<
                    oplogEntry;
            }
        }

    }  // namespace

    Status AuthorizationManager::initializeRoleGraph() {
        boost::lock_guard<boost::mutex> lkInitialzeRoleGraph(_initializeRoleGraphMutex);
        {
            boost::lock_guard<boost::mutex> lkLocal(_lock);
            switch (_roleGraphState) {
            case roleGraphStateInitial:
            case roleGraphStateHasCycle:
                break;
            case roleGraphStateConsistent:
                return Status(ErrorCodes::AlreadyInitialized,
                              "Role graph already initialized and consistent.");
            default:
                return Status(ErrorCodes::InternalError,
                              mongoutils::str::stream() << "Invalid role graph state " <<
                              _roleGraphState);
            }
        }

        RoleGraph newRoleGraph;
        const OpTime t0 = _externalState->getCurrentOpTime();
        Status status = _externalState->query(
                rolesCollectionName,
                BSONObj(),
                boost::bind(addRoleFromDocumentOrWarn, &newRoleGraph, _1));
        if (!status.isOK())
            return status;

        const OpTime t1 = _externalState->getCurrentOpTime();
        status = _externalState->query(
                _externalState->getOplogCollectionName(),
                BSON("ts" << BSON("$gte" << t0 << "$lte" << t1) <<
                     "ns" << BSON("$in" << BSON_ARRAY(rolesCollectionName.ns() <<
                                                      adminCommandNamespace.ns()))),
                boost::bind(updateRoleGraphFromOplogOrWarn, &newRoleGraph, _1));
        if (!status.isOK())
            return status;
        status = newRoleGraph.recomputePrivilegeData();

        RoleGraphState newState;
        if (status == ErrorCodes::GraphContainsCycle) {
            error() << "Inconsistent role graph during authorization manager intialization.  Only "
                "direct privileges available. " << status.reason();
            newState = roleGraphStateHasCycle;
            status = Status::OK();
        }
        else if (status.isOK()) {
            newState = roleGraphStateConsistent;
        }
        else {
            newState = roleGraphStateInitial;
            newRoleGraph = RoleGraph();
        }

        if (status.isOK()) {
            boost::lock_guard<boost::mutex> lkLocal(_lock);
            _roleGraph.swap(newRoleGraph);
            _roleGraphState = newState;
        }
        return status;
    }

    void AuthorizationManager::logOp(
            const char* op,
            const char* ns,
            const BSONObj& o,
            BSONObj* o2,
            bool* b,
            bool fromMigrateUnused,
            const BSONObj* fullObjUnused) {

        if (ns == rolesCollectionName.ns() || ns == adminCommandNamespace.ns()) {
            boost::lock_guard<boost::mutex> lk(_lock);
            Status status = updateRoleGraphWithLogOpSignature(
                    &_roleGraph, op, NamespaceString(ns), o, o2);

            if (status == ErrorCodes::OplogOperationUnsupported) {
                _roleGraph = RoleGraph();
                _roleGraphState = roleGraphStateInitial;
                error() << "Unsupported modification to roles collection in oplog; "
                    "TODO how to remedy. " << status << " Oplog entry: " << op;
            }
            else if (!status.isOK()) {
                warning() << "Skipping bad update to roles collection in oplog. " << status <<
                    " Oplog entry: " << op;
            }
            status = _roleGraph.recomputePrivilegeData();
            if (status == ErrorCodes::GraphContainsCycle) {
                error() << "Inconsistent role graph during authorization manager intialization.  "
                    "Only direct privileges available. " << status.reason() <<
                    " after applying oplog entry " << op;
                _roleGraphState = roleGraphStateHasCycle;
            }
            else if (!status.isOK()) {
                error() << "Error updating role graph; only builtin roles available. "
                    "TODO how to remedy. " << status << " Oplog entry: " << op;
                _roleGraphState = roleGraphStateInitial;
            }
            else {
                _roleGraphState = roleGraphStateConsistent;
            }
        }
    }

} // namespace mongo
