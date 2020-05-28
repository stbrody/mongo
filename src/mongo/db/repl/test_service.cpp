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

#include "mongo/db/repl/test_service.h"

#include <memory>

#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/oid.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/logical_time_metadata_hook.h"
#include "mongo/db/repl/primary_only_service.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/test_type_gen.h"
#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/metadata/egress_metadata_hook_list.h"
#include "mongo/util/concurrency/thread_pool.h"

namespace mongo {
namespace repl {

const std::string TestService::kServiceName = "testService";

namespace {
const std::string kThreadNamePrefix = "TestServiceThread-";
const std::string kPoolName = "TestServiceThreadPool";
const std::string kNetworkName = "TestServiceNetwork";
}  // namespace

std::unique_ptr<executor::TaskExecutor> TestService::makeTaskExecutor(
    ServiceContext* serviceContext) {
    ThreadPool::Options threadPoolOptions;
    threadPoolOptions.threadNamePrefix = kThreadNamePrefix;
    threadPoolOptions.poolName = kPoolName;
    threadPoolOptions.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
        AuthorizationSession::get(cc())->grantInternalAuthorization(&cc());
    };
    auto pool = std::make_unique<ThreadPool>(threadPoolOptions);

    auto hookList = std::make_unique<rpc::EgressMetadataHookList>();
    hookList->addHook(std::make_unique<rpc::LogicalTimeMetadataHook>(serviceContext));
    auto networkName = kNetworkName;
    return std::make_unique<executor::ThreadPoolTaskExecutor>(
        std::move(pool), executor::makeNetworkInterface(networkName, nullptr, std::move(hookList)));
}

void TestService::initialize(const BSONObj& state) {
    _state = TestStruct::parse(IDLParserErrorContext("parsing test type"), state);
}

OpTime TestService::runOnceImpl(OperationContext* opCtx) {
    auto newState = _state.getMyState();
    LOGV2(0, "####### state: {state}", "state"_attr = newState);
    switch (_state.getMyState()) {
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
    _state.setMyState(newState);

    sleepmillis(1000);

    BSONObj stateObj = _state.toBSON();
    TimestampedBSONObj update;
    update.obj = stateObj;

    auto storage = StorageInterface::get(opCtx);
    uassertStatusOK(storage->putSingleton(opCtx, TestService::ns(), update));

    return ReplClientInfo::forClient(opCtx->getClient()).getLastOp();
}

class TestServiceCommand : public BasicCommand {
public:
    TestServiceCommand() : BasicCommand("testService") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
    std::string help() const override {
        return "starts up a new instance of the test service.";
    }
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }
    virtual bool allowsAfterClusterTime(const BSONObj& cmdObj) const override {
        return false;
    }
    virtual void addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) const {}  // No auth required
    virtual bool requiresAuth() const override {
        return false;
    }
    virtual bool run(OperationContext* opCtx,
                     const std::string& ns,
                     const BSONObj& cmdObj,
                     BSONObjBuilder& result) {
        auto registry = PrimaryOnlyServiceRegistry::get(opCtx->getServiceContext());
        auto service = registry->lookupService(TestService::kServiceName);
        invariant(service);

        TestStruct initialState;
        initialState.setMyState(TestServiceStateEnum::kStateFoo);
        initialState.set_id(OID::gen());

        auto storage = StorageInterface::get(opCtx);
        auto status = storage->createCollection(opCtx, TestService::ns(), CollectionOptions());
        if (status != ErrorCodes::NamespaceExists) {
            uassertStatusOK(status);
        }

        TimestampedBSONObj update;
        update.obj = initialState.toBSON();
        uassertStatusOK(storage->putSingleton(opCtx, TestService::ns(), update));

        service->startNewInstance(std::move(update.obj),
                                  ReplClientInfo::forClient(opCtx->getClient()).getLastOp());
        return true;
    }
} pingCmd;

}  // namespace repl
}  // namespace mongo
