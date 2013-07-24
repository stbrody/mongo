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

/**
 * Unit tests of the UserSet type.
 */

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/auth/user_set.h"
#include "mongo/unittest/unittest.h"

#define ASSERT_NULL(EXPR) ASSERT_FALSE((EXPR))

namespace mongo {

    static inline std::ostream& operator<<(std::ostream& os, const UserName& uname) {
        return os << uname.toString();
    }

namespace {

    class UserSetTest : public ::mongo::unittest::Test {
    public:
        AuthzManagerExternalStateMock* managerState;
        scoped_ptr<AuthorizationManager> authzManager;

        void setUp() {
            managerState = new AuthzManagerExternalStateMock();
            authzManager.reset(new AuthorizationManager(managerState));
        }
    };

    TEST_F(UserSetTest, BasicTest) {
        UserSet set(authzManager.get());

        User* p1 = new User(UserName("Bob", "test"));
        User* p2 = new User(UserName("George", "test"));
        User* p3 = new User(UserName("Bob", "test2"));

        ASSERT_NULL(set.lookup(UserName("Bob", "test")));
        ASSERT_NULL(set.lookup(UserName("George", "test")));
        ASSERT_NULL(set.lookup(UserName("Bob", "test2")));
        ASSERT_NULL(set.lookupByDBName("test"));
        ASSERT_NULL(set.lookupByDBName("test2"));

        set.add(p1);

        ASSERT_EQUALS(p1, set.lookup(UserName("Bob", "test")));
        ASSERT_EQUALS(p1, set.lookupByDBName("test"));
        ASSERT_NULL(set.lookup(UserName("George", "test")));
        ASSERT_NULL(set.lookup(UserName("Bob", "test2")));
        ASSERT_NULL(set.lookupByDBName("test2"));

        // This should not replace the existing user "Bob" because they are different databases
        set.add(p3);

        ASSERT_EQUALS(p1, set.lookup(UserName("Bob", "test")));
        ASSERT_EQUALS(p1, set.lookupByDBName("test"));
        ASSERT_NULL(set.lookup(UserName("George", "test")));
        ASSERT_EQUALS(p3, set.lookup(UserName("Bob", "test2")));
        ASSERT_EQUALS(p3, set.lookupByDBName("test2"));

        set.add(p2); // This should replace Bob since they're on the same database

        ASSERT_NULL(set.lookup(UserName("Bob", "test")));
        ASSERT_EQUALS(p2, set.lookup(UserName("George", "test")));
        ASSERT_EQUALS(p2, set.lookupByDBName("test"));
        ASSERT_EQUALS(p3, set.lookup(UserName("Bob", "test2")));
        ASSERT_EQUALS(p3, set.lookupByDBName("test2"));

        set.removeByDBName("test");

        ASSERT_NULL(set.lookup(UserName("Bob", "test")));
        ASSERT_NULL(set.lookup(UserName("George", "test")));
        ASSERT_NULL(set.lookupByDBName("test"));
        ASSERT_EQUALS(p3, set.lookup(UserName("Bob", "test2")));
        ASSERT_EQUALS(p3, set.lookupByDBName("test2"));
    }

    TEST_F(UserSetTest, IterateNames) {
        UserSet pset(authzManager.get());
        UserSet::NameIterator iter = pset.getNames();
        ASSERT(!iter.more());

        pset.add(new User(UserName("bob", "test")));

        iter = pset.getNames();
        ASSERT(iter.more());
        ASSERT_EQUALS(*iter, UserName("bob", "test"));
        ASSERT_EQUALS(iter.next(), UserName("bob", "test"));
        ASSERT(!iter.more());
    }

}  // namespace
}  // namespace mongo
