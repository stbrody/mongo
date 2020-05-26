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
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/test_type_gen.h"
#include "mongo/executor/task_executor.h"

#include <memory>
#include <vector>

//#include "mongo/db/repl/primary_aware_service.h"

namespace mongo {
namespace repl {

class PrimaryOnlyServiceInstance {
public:
    explicit PrimaryOnlyServiceInstance(long long term) : _term(term) {}
    virtual ~PrimaryOnlyServiceInstance() = default;

    /**
     * Initialize any needed initial state based on the state document provided.
     * Must not block.
     * Throws on error.
     */
    virtual void initialize(BSONObj state) = 0;

    /**
     * The XXXXX mechanism will call this repeatedly as long as this node remains primary in _term.
     * Can block and do disk AND network i/o.
     * Is responsible for persisting any state needed so that its work can be resumed if it is
     * killed and restarted as a new instance.  Returns the OpTime that must be visible for
     * subsequent calls to startup with a 'state' object read at that optime to resume this work.
     * Throws on error.
     */
    OpTime runOnce(OperationContext* opCtx) {
        return runOnceImpl(opCtx);
    }

protected:
    virtual OpTime runOnceImpl(OperationContext* opCtx) = 0;

    const long long _term;
};

class TestService : public PrimaryOnlyServiceInstance {
public:
    explicit TestService(long long term) : PrimaryOnlyServiceInstance(term) {}
    virtual ~TestService() = default;

    void initialize(BSONObj state) final {
        myStateStruct = TestStruct::parse(IDLParserErrorContext("parsing test type"), state);
    }

    OpTime runOnceImpl(OperationContext* opCtx) final {
        auto storage = StorageInterface::get(opCtx);

        auto newState = myStateStruct.getMyState();
        switch (myStateStruct.getMyState()) {
            case TestServiceStateEnum::kStateFoo:
                newState = TestServiceStateEnum::kStateBar;
                break;
            case TestServiceStateEnum::kStateBar:
                newState = TestServiceStateEnum::kStateBaz;
                break;
            case TestServiceStateEnum::kStateBaz:
                newState = TestServiceStateEnum::kStateFoo;
                break;
        }
        myStateStruct.setMyState(newState);

        BSONObj stateObj = myStateStruct.toBSON();
        TimestampedBSONObj update;
        update.obj = stateObj;

        uassertStatusOK(storage->updateSingleton(
            opCtx, NamespaceString("admin.myservice"), stateObj.getField("_id").wrap(), update));

        return ReplClientInfo::forClient(opCtx->getClient()).getLastOp();
    }

private:
    TestStruct myStateStruct;
};

class PrimaryOnlyServiceGroup {
private:
    using ConstructInstanceFn =
        std::function<std::shared_ptr<PrimaryOnlyServiceInstance>(long long)>;

public:
    PrimaryOnlyServiceGroup(executor::TaskExecutor* executor,
                            NamespaceString ns,
                            ConstructInstanceFn constructInstanceFn)
        : _executor(executor), _ns(ns), _constructInstanceFn(constructInstanceFn) {}
    virtual ~PrimaryOnlyServiceGroup() = default;

    void startup(long long term) {
        auto res = _executor->scheduleWork(
            [this, term](const mongo::executor::TaskExecutor::CallbackArgs& args) {
                uassertStatusOK(args.status);  // todo is throwing like this safe?
                _startup(args.opCtx, term);
            });
        auto handle = uassertStatusOK(res);  // throw away handle, not needed.
    }

private:
    void _startup(OperationContext* opCtx, long long term) {
        auto storage = StorageInterface::get(opCtx);
        const auto docs =
            uassertStatusOK(storage->findAllDocuments(opCtx, _ns));  // todo safe to throw/uassert?
        for (const auto& doc : docs) {
            std::cout << doc.toString() << std::endl;

            _instances.push_back(_constructInstanceFn(term));
            _instances.back()->initialize(doc);
            // todo start scheduling tasks to call 'runOnce' on instances.
        }
    }

    executor::TaskExecutor* _executor;  // todo: who owns this?

    // Namespace where docs containing state for instances of this service group are stored.
    const NamespaceString _ns;

    const ConstructInstanceFn _constructInstanceFn;

    std::vector<std::shared_ptr<PrimaryOnlyServiceInstance>> _instances;
};

class PrimaryOnlyServiceRegistry {  // todo make this a ReplicaSetAwareService
public:
    PrimaryOnlyServiceRegistry() {}
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

MONGO_INITIALIZER(RegisterPrimaryOnlyServices)(InitializerContext*) {
    PrimaryOnlyServiceRegistry registry;  // TODO make this a service context decoration

    auto group = std::make_unique<PrimaryOnlyServiceGroup>(
        nullptr, NamespaceString("admin.myservice"), [](long long term) {
            return std::make_shared<TestService>(term);
        });

    registry.registerServiceGroup(std::move(group));
    return Status::OK();
}

}  // namespace repl
}  // namespace mongo
