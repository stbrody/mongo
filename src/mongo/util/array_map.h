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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <tuple>
#include <vector>

namespace mongo {

template <typename K, typename V>
class ArrayMap {
public:
    using value_type = std::tuple<K, V>;
    using iterator = typename std::vector<value_type>::iterator;

    ArrayMap() = default;
    ArrayMap(std::initializer_list<value_type> values) : _data(values) {}

    bool empty() const {
        return _data.empty();
    }

    size_t size() const {
        return _data.size();
    }

    iterator begin() {
        return _data.begin();
    }

    iterator end() {
        return _data.end();
    }

    iterator find(const K& key) {
        return std::find_if(_data.begin(), _data.end(), [&key](const value_type& entry) {
            return std::get<0>(entry) == key;
        });
    }

    V& operator[](const K& key) {
        auto it = find(key);
        if (it == end()) {
            _data.push_back(std::make_tuple(key, V{}));
            return std::get<1>(_data.back());
        } else {
            return std::get<1>(*it);
        }
    }

private:
    std::vector<value_type> _data;
};

}  // namespace mongo
