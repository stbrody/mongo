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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/test_type_gen.h"
#include "mongo/executor/task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

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
    void runOnce(OperationContext* opCtx) {
        // TODO write concern handling.
        runOnceImpl(opCtx);
    }

protected:
    virtual OpTime runOnceImpl(OperationContext* opCtx) = 0;

    const long long _term;

    OpTime _optime;  // todo comment
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
    using ConstructExecutorFn = std::function<std::unique_ptr<executor::TaskExecutor>()>;
    using ConstructInstanceFn =
        std::function<std::shared_ptr<PrimaryOnlyServiceInstance>(long long)>;

public:
    PrimaryOnlyServiceGroup(NamespaceString ns,
                            ConstructInstanceFn constructInstanceFn,
                            ConstructExecutorFn constructExecutorFn)
        : _ns(ns),
          _constructInstanceFn(constructInstanceFn),
          _constructExecutorFn(constructExecutorFn) {}
    virtual ~PrimaryOnlyServiceGroup() = default;

    void startup(long long term) {
        _executor = _constructExecutorFn();
        _executor->startup();

        auto res = _executor->scheduleWork(
            [this, term](const mongo::executor::TaskExecutor::CallbackArgs& args) {
                uassertStatusOK(args.status);  // todo error handling
                _startup(args.opCtx, term);
            });
        auto handle = uassertStatusOK(res);  // throw away handle, not needed.
    }

    void shutdown() {
        _executor->shutdown();
        _executor.reset();
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

            // start scheduling tasks to repeatedly call 'runOnce' on instances.
            auto res = _executor->scheduleWork(
                [this, instance = _instances.back()](
                    const mongo::executor::TaskExecutor::CallbackArgs& args) {
                    _taskInstanceRunner(args, instance);
                });
            auto handle =
                uassertStatusOK(res);  // throw away handle, not needed. TODO error handling.
        }
    }

    /**
     * Codes that happen during stepdown or shutdown.
     */
    static bool isExpected(Status status) {
        return ErrorCodes::isNotMasterError(status.code()) ||
            ErrorCodes::isShutdownError(status.code()) ||
            ErrorCodes::CallbackCanceled == status.code();
    }

    void _taskInstanceRunner(const mongo::executor::TaskExecutor::CallbackArgs& args,
                             std::shared_ptr<PrimaryOnlyServiceInstance> instance) noexcept {
        Status status = args.status.isOK() ? args.opCtx->checkForInterruptNoAssert() : args.status;
        if (!status.isOK()) {
            if (isExpected(status)) {
                LOGV2_DEBUG(0,
                            2,
                            "Received error in primary-only service, this is expected during "
                            "stepdown or shutdown. {status}",
                            "status"_attr = status);
                return;
            }
            LOGV2_DEBUG(0,
                        2,
                        "Received unexpected error in primary-only service. Retrying. {status}",
                        "status"_attr = status);
        } else {
            try {
                instance->runOnce(args.opCtx);
            } catch (const DBException& e) {
                if (isExpected(e.toStatus())) {
                    LOGV2_DEBUG(0,
                                2,
                                "got error that's expected during stepdown. {status}",
                                "status"_attr = status);
                    return;
                }
                LOGV2_DEBUG(0,
                            2,
                            "Got error that's not expected, retrying. {status}",
                            "status"_attr = status);
            }
        }

        // If we got this far this means we never encountered a non-recoverable error and thus
        // should go ahead and schedule the next iteration of this task.
        auto res =
            _executor->scheduleWork([this, instance = _instances.back()](
                                        const mongo::executor::TaskExecutor::CallbackArgs& args) {
                _taskInstanceRunner(args, instance);
            });
        if (!res.isOK()) {
            if (isExpected(res.getStatus())) {
                LOGV2_DEBUG(0,
                            2,
                            "got error that's expected during stepdown. {status}",
                            "status"_attr = status);
                return;
            }
        }
        invariantStatusOK(res);
    }

    // Namespace where docs containing state for instances of this service group are stored.
    const NamespaceString _ns;

    // Function for constructing new instances of this ServiceGroup.
    const ConstructInstanceFn _constructInstanceFn;

    // Function for constructing new a TaskExecutor for each term in which this node is primary.
    const ConstructExecutorFn _constructExecutorFn;

    std::unique_ptr<executor::TaskExecutor> _executor;

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
        NamespaceString("admin.myservice"),
        [](long long term) { return std::make_shared<TestService>(term); },
        []() {
            // todo make a real executor.  Must run in a compilation unit linked against Client and
            // AuthorizationSession.
            std::unique_ptr<executor::TaskExecutor> executor;
            return executor;
        });

    registry.registerServiceGroup(std::move(group));
    return Status::OK();
}

}  // namespace repl
}  // namespace mongo
