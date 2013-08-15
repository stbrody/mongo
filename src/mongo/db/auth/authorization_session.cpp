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
*/

#include "mongo/db/auth/authorization_session.h"

#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authz_session_external_state.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/security_key.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace {
    const std::string ADMIN_DBNAME = "admin";
}  // namespace

    AuthorizationSession::AuthorizationSession(AuthzSessionExternalState* externalState) {
        _externalState.reset(externalState);
    }

    AuthorizationSession::~AuthorizationSession() {
        for (UserSet::iterator it = _authenticatedUsers.begin();
                it != _authenticatedUsers.end(); ++it) {
            getAuthorizationManager().releaseUser(*it);
        }
    }

    AuthorizationManager& AuthorizationSession::getAuthorizationManager() {
        return _externalState->getAuthorizationManager();
    }

    void AuthorizationSession::startRequest() {
        _externalState->startRequest();
    }

    Status AuthorizationSession::addAndAuthorizeUser(const UserName& userName) {
        User* user;
        Status status = getAuthorizationManager().acquireUser(userName, &user);
        if (!status.isOK()) {
            return status;
        }

        // Calling add() on the UserSet may return a user that was replaced because it was from the
        // same database.
        User* replacedUser = _authenticatedUsers.add(user);
        if (replacedUser) {
            getAuthorizationManager().releaseUser(replacedUser);
        }

        _externalState->onAddAuthorizedUser();
        return Status::OK();
    }

    User* AuthorizationSession::lookupUser(const UserName& name) {
        return _authenticatedUsers.lookup(name);
    }

    void AuthorizationSession::logoutDatabase(const std::string& dbname) {
        User* removedUser = _authenticatedUsers.removeByDBName(dbname);
        if (removedUser) {
            getAuthorizationManager().releaseUser(removedUser);
        }

        _externalState->onLogoutDatabase(dbname);
    }

    UserSet::NameIterator AuthorizationSession::getAuthenticatedUserNames() {
        return _authenticatedUsers.getNames();
    }

    void AuthorizationSession::grantInternalAuthorization() {
        _authenticatedUsers.add(internalSecurity.user);
    }

    bool AuthorizationSession::hasInternalAuthorization() {
        ActionSet allActions;
        allActions.addAllActions();
        return _checkAuthForPrivilegeHelper(
                Privilege(AuthorizationManager::WILDCARD_RESOURCE_NAME, allActions)).isOK();
    }

    bool AuthorizationSession::checkAuthorization(const std::string& resource,
                                                  ActionType action) {
        return checkAuthForPrivilege(Privilege(resource, action)).isOK();
    }

    bool AuthorizationSession::checkAuthorization(const std::string& resource,
                                                  ActionSet actions) {
        return checkAuthForPrivilege(Privilege(resource, actions)).isOK();
    }

    Status AuthorizationSession::checkAuthForQuery(const std::string& ns, const BSONObj& query) {
        NamespaceString namespaceString(ns);
        verify(!namespaceString.isCommand());
        if (!checkAuthorization(ns, ActionType::find)) {
            return Status(ErrorCodes::Unauthorized,
                          mongoutils::str::stream() << "not authorized for query on " << ns,
                          0);
        }
        return Status::OK();
    }

    Status AuthorizationSession::checkAuthForGetMore(const std::string& ns, long long cursorID) {
        if (!checkAuthorization(ns, ActionType::find)) {
            return Status(ErrorCodes::Unauthorized,
                          mongoutils::str::stream() << "not authorized for getmore on " << ns,
                          0);
        }
        return Status::OK();
    }

    Status AuthorizationSession::checkAuthForInsert(const std::string& ns,
                                                    const BSONObj& document) {
        NamespaceString namespaceString(ns);
        if (namespaceString.coll() == StringData("system.indexes", StringData::LiteralTag())) {
            std::string indexNS = document["ns"].String();
            if (!checkAuthorization(indexNS, ActionType::ensureIndex)) {
                return Status(ErrorCodes::Unauthorized,
                              mongoutils::str::stream() << "not authorized to create index on " <<
                                      indexNS,
                              0);
            }
        } else {
            if (!checkAuthorization(ns, ActionType::insert)) {
                return Status(ErrorCodes::Unauthorized,
                              mongoutils::str::stream() << "not authorized for insert on " << ns,
                              0);
            }
        }

        return Status::OK();
    }

    Status AuthorizationSession::checkAuthForUpdate(const std::string& ns,
                                                    const BSONObj& query,
                                                    const BSONObj& update,
                                                    bool upsert) {
        NamespaceString namespaceString(ns);
        if (!upsert) {
            if (!checkAuthorization(ns, ActionType::update)) {
                return Status(ErrorCodes::Unauthorized,
                              mongoutils::str::stream() << "not authorized for update on " << ns,
                              0);
            }
        }
        else {
            ActionSet required;
            required.addAction(ActionType::update);
            required.addAction(ActionType::insert);
            if (!checkAuthorization(ns, required)) {
                return Status(ErrorCodes::Unauthorized,
                              mongoutils::str::stream() << "not authorized for upsert on " << ns,
                              0);
            }
        }
        return Status::OK();
    }

    Status AuthorizationSession::checkAuthForDelete(const std::string& ns, const BSONObj& query) {
        NamespaceString namespaceString(ns);
        if (!checkAuthorization(ns, ActionType::remove)) {
            return Status(ErrorCodes::Unauthorized,
                          mongoutils::str::stream() << "not authorized to remove from " << ns,
                          0);
        }
        return Status::OK();
    }

    Privilege AuthorizationSession::_modifyPrivilegeForSpecialCases(const Privilege& privilege) {
        ActionSet newActions;
        newActions.addAllActionsFromSet(privilege.getActions());
        NamespaceString ns( privilege.getResource() );

        if (ns.coll() == "system.users") {
            if (newActions.contains(ActionType::insert) ||
                    newActions.contains(ActionType::update) ||
                    newActions.contains(ActionType::remove)) {
                // End users can't modify system.users directly, only the system can.
                newActions.addAction(ActionType::userAdminV1);
            } else {
                newActions.addAction(ActionType::userAdmin);
            }
            newActions.removeAction(ActionType::find);
            newActions.removeAction(ActionType::insert);
            newActions.removeAction(ActionType::update);
            newActions.removeAction(ActionType::remove);
        } else if (ns.coll() == "system.profile") {
            newActions.removeAction(ActionType::find);
            newActions.addAction(ActionType::profileRead);
        } else if (ns.coll() == "system.indexes" && newActions.contains(ActionType::find)) {
            newActions.removeAction(ActionType::find);
            newActions.addAction(ActionType::indexRead);
        }

        return Privilege(privilege.getResource(), newActions);
    }

    Status AuthorizationSession::checkAuthForPrivilege(const Privilege& privilege) {
        if (_externalState->shouldIgnoreAuthChecks())
            return Status::OK();

        return _checkAuthForPrivilegeHelper(privilege);
    }

    Status AuthorizationSession::checkAuthForPrivileges(const vector<Privilege>& privileges) {
        if (_externalState->shouldIgnoreAuthChecks())
            return Status::OK();

        for (size_t i = 0; i < privileges.size(); ++i) {
            Status status = _checkAuthForPrivilegeHelper(privileges[i]);
            if (!status.isOK())
                return status;
        }

        return Status::OK();
    }

    Status AuthorizationSession::_checkAuthForPrivilegeHelper(const Privilege& privilege) {
        Privilege modifiedPrivilege = _modifyPrivilegeForSpecialCases(privilege);

        // Need to check not just the resource of the privilege, but also just the database
        // component and the "*" resource.
        std::string resourceSearchList[3];
        resourceSearchList[0] = AuthorizationManager::WILDCARD_RESOURCE_NAME;
        resourceSearchList[1] = nsToDatabase(modifiedPrivilege.getResource());
        resourceSearchList[2] = modifiedPrivilege.getResource();


        ActionSet unmetRequirements = modifiedPrivilege.getActions();
        for (UserSet::iterator it = _authenticatedUsers.begin();
                it != _authenticatedUsers.end(); ++it) {
            User* user = *it;

            // TODO(spencer): Handle if the user has been invalidated.

            for (int i = 0; i < static_cast<int>(boost::size(resourceSearchList)); ++i) {
                ActionSet userActions = user->getActionsForResource(resourceSearchList[i]);
                unmetRequirements.removeAllActionsFromSet(userActions);

                if (unmetRequirements.empty())
                    return Status::OK();
            }
        }

        return Status(ErrorCodes::Unauthorized, "unauthorized");
    }

} // namespace mongo
