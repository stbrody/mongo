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
#include <map>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/db/auth/authz_manager_external_state.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/namespacestring.h"

namespace mongo {

    /**
     * Mock of the AuthzManagerExternalState class used only for testing.
     */
    class AuthzManagerExternalStateMock : public AuthzManagerExternalState {
        MONGO_DISALLOW_COPYING(AuthzManagerExternalStateMock);

    public:

        AuthzManagerExternalStateMock() {};

        virtual Status insertPrivilegeDocument(const std::string& dbname,
                                               const BSONObj& userObj) const {
            return Status::OK();
        }

        // Non-const version that puts document into a vector that can be accessed later
        Status insertPrivilegeDocument(const std::string& dbname, const BSONObj& userObj) {
            _userDocuments.insert(make_pair(dbname, userObj));
            return Status::OK();
        }

        void clearPrivilegeDocuments() {
            _userDocuments.clear();
        }

        virtual Status updatePrivilegeDocument(const UserName& user,
                                               const BSONObj& updateObj) const {
            return Status::OK();
        }

        virtual bool _findUser(const std::string& usersNamespace,
                               const BSONObj& query,
                               BSONObj* result) const {
            StatusWithMatchExpression parseResult = MatchExpressionParser::parse(query);
            if (!parseResult.isOK()) {
                return false;
            }
            MatchExpression* matcher = parseResult.getValue();

            for (map<std::string, BSONObj>::const_iterator it = _userDocuments.begin();
                    it != _userDocuments.end(); ++it) {
                if (nsToDatabase(usersNamespace) == it->first && matcher->matchesBSON(it->second)) {
                    *result = it->second;
                    return true;
                }
            }
            return false;
        }

    private:
        map<std::string, BSONObj> _userDocuments; // dbname to user document
    };

} // namespace mongo
