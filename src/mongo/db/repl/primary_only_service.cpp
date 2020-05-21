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
#include "mongo/db/repl/storage_interface.h"
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

class PrimaryOnlyServiceGroup {
public:
    explicit PrimaryOnlyServiceGroup(executor::TaskExecutor* executor) : _executor(executor) {}
    virtual ~PrimaryOnlyServiceGroup() = default;

    void startup(long long term) {
        auto res = _executor->scheduleWork(
            [this, term](const mongo::executor::TaskExecutor::CallbackArgs& args) {
                uassertStatusOK(args.status);  // todo is throwing like this safe?
                _startup(args.opCtx, term);
            });
        auto handle = uassertStatusOK(res);  // throw away handle, not needed.
    }

    /**
     * Returns the namespace where this service group keeps the documents containing the state for
     * its individual service instances.
     */
    virtual NamespaceString getNamespace() const = 0;

private:
    void _startup(OperationContext* opCtx, long long term) {
        auto storage = StorageInterface::get(opCtx);
        const auto docs = uassertStatusOK(
            storage->findAllDocuments(opCtx, getNamespace()));  // todo safe to throw?
    }

protected:
    executor::TaskExecutor* _executor;  // todo: who owns this?
};

class TestService : public PrimaryOnlyServiceGroup {
public:
    TestService(executor::TaskExecutor* executor, NamespaceString ns)
        : PrimaryOnlyServiceGroup(executor), _ns(ns){};

    // TODO can we move all namespace consideration to the parent class?
    NamespaceString getNamespace() const final {
        return _ns;
    }

    void test(BSONObj bson) {
        auto test = TestStruct::parse(IDLParserErrorContext("parsing test type"), bson);
    }

private:
    // Namespace where docs containing state about instances of this service group are stored.
    NamespaceString _ns;
};

class PrimaryOnlyServiceRegistry {
public:
    /**
     * Iterates over all registered services and starts them up.
     */
    void onStepUp(long long term) {
        for (auto& service : _services) {
            service->startup(term);
        }
    }

    void registerServiceGroup(std::unique_ptr<PrimaryOnlyServiceGroup> service) {
        _services.push_back(std::move(service));
    }

private:
    std::vector<std::unique_ptr<PrimaryOnlyServiceGroup>> _services;
};

}  // namespace repl
}  // namespace mongo
