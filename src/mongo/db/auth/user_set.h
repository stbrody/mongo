/*    Copyright 2012 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"


namespace mongo {

    /**
     * A collection of authenticated users.
     * This class does not do any locking/synchronization, the consumer will be responsible for
     * synchronizing access.
     */
    class UserSet {
        MONGO_DISALLOW_COPYING(UserSet);
    public:
        typedef std::vector<User*>::const_iterator iterator;

        /**
         * Forward iterator over the names of the users stored in a UserSet.
         *
         * Instances are valid until the underlying vector<User*> is modified.
         *
         * more() must be the first method called after construction, and must be checked
         * after each call to next() before calling any other methods.
         */
        class NameIterator {
        public:
            explicit NameIterator(const std::vector<User*>& users) :
                _curr(users.begin()),
                _end(users.end()) {
            }

            NameIterator() {}

            bool more() { return _curr != _end; }
            const UserName& next() {
                const UserName& ret = get();
                ++_curr;
                return ret;
            }

            const UserName& get() const { return (*_curr)->getName(); }

            const UserName& operator*() const { return get(); }
            const UserName* operator->() const { return &get(); }

        private:
            std::vector<User*>::const_iterator _curr;
            std::vector<User*>::const_iterator _end;
        };

        UserSet(AuthorizationManager* authzManager);
        ~UserSet();

        // If the user is already present, this will replace the existing entry.
        // The UserSet takes ownership of the passed-in user and is responsible for
        // deleting it eventually
        void add(User* user);

        // Removes all users whose authentication credentials came from dbname.
        void removeByDBName(const StringData& dbname);

        // Returns the User with the given name, or NULL if not found.
        // Ownership of the returned User remains with the UserSet.  The pointer
        // returned is only guaranteed to remain valid until the next non-const method is called
        // on the UserSet.
        User* lookup(const UserName& name) const;

        // Gets the user whose authentication credentials came from dbname, or NULL if none
        // exist.  There should be at most one such user.
        User* lookupByDBName(const StringData& dbname) const;

        // Gets an iterator over the names of the users stored in the set.  The iterator is
        // valid until the next non-const method is called on the UserSet.
        NameIterator getNames() const { return NameIterator(_users); }

        iterator begin() const { return _users.begin(); }
        iterator end() const { return _users.end(); }

    private:
        // The UserSet maintains ownership of the Users in it, and is responsible for
        // returning them to the AuthorizationManager when done with them.
        std::vector<User*> _users;

        // Pointer to the AuthorizationManager that owns the underlying Users, so the UserSet can
        //release Users back to it when it's done with them.
        AuthorizationManager* _authzManager;
    };

} // namespace mongo
