/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kRepl

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/test_type_gen.h"
#include "mongo/executor/task_executor.h"


#include <vector>

//#include "mongo/db/repl/primary_aware_service.h"

namespace mongo {
namespace repl {
MONGO_INITIALIZER(RegisterPrimaryOnlyServices)(InitializerContext*) {
    // TODO Try registering an example service
    return Status::OK();
}

class PrimaryOnlyServiceGroup;

class PrimaryOnlyServiceRegistry {
public:
    void onStepUp(long long term) {
        // iterate over service groups, start them up.
    }

    void registerServiceGroup() {}

private:
    std::vector<PrimaryOnlyServiceGroup> _services;
};

class PrimaryOnlyServiceGroup {
public:
    explicit PrimaryOnlyServiceGroup(executor::TaskExecutor* executor, NamespaceString ns)
        : _executor(executor), _ns(ns){};

    void startup(BSONObj bson) {
        auto test = TestStruct::parse(IDLParserErrorContext("parsing test type"), bson);
    }

private:
    executor::TaskExecutor* _executor;  // todo: who owns this?

    // Namespace where docs containing state about instances of this service group are stored.
    NamespaceString _ns;
};

}  // namespace repl
}  // namespace mongo
