/**
 *    Copyright (C) 2016 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include <iostream>  // TODO: remove
#include <string>

#include "mongo/unittest/unittest.h"
#include "mongo/util/array_map.h"

namespace mongo {


TEST(ArrayMapTest, InitializerListConstruction) {
    ArrayMap<std::string, std::string> map{{"a", "A"}, {"b", "B"}, {"c", "C"}};
    ASSERT_EQ(3U, map.size());
    ASSERT_EQ("A", map["a"]);
    ASSERT_EQ("B", map["b"]);
    ASSERT_EQ("C", map["c"]);
}

TEST(ArrayMapTest, InsertViaOperatorAt) {
    ArrayMap<int, std::string> map;
    ASSERT_TRUE(map.empty());
    ASSERT_EQ(0U, map.size());

    map[1] = "hello";
    map[3] = "bob";

    ASSERT_EQ("hello", map[1]);
    ASSERT_EQ("", map[2]);
    ASSERT_EQ("bob", map[3]);

    ASSERT_FALSE(map.empty());
    ASSERT_EQ(3U, map.size());
}

TEST(ArrayMapTest, Iteration) {
    ArrayMap<int, int> map{{1, 11}, {2, 12}, {3, 13}};
    for (auto& entry : map) {
        ASSERT_EQ(entry.first + 10, entry.second);
    }

    for (const auto& entry : map) {
        ASSERT_EQ(entry.first + 10, entry.second);
    }
}

}  // namespace mongo
