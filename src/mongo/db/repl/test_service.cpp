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
#include "mongo/db/repl/primary_only_service.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/test_type_gen.h"
#include "mongo/executor/task_executor.h"
#include "mongo/logv2/log.h"

namespace mongo {
namespace repl {

std::unique_ptr<executor::TaskExecutor> TestService::makeTaskExecutor() {
    // todo make a real executor.  Must run in a compilation unit
    // linked against Client and AuthorizationSession.
    std::unique_ptr<executor::TaskExecutor> executor;
    return executor;
}

void TestService::initialize(BSONObj state) {
    myStateStruct = TestStruct::parse(IDLParserErrorContext("parsing test type"), state);
}

OpTime TestService::runOnceImpl(OperationContext* opCtx) {
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

}  // namespace repl
}  // namespace mongo
