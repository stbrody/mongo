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

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/optime.h"
#include "mongo/executor/task_executor.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/concurrency/with_lock.h"

#include <boost/optional.hpp>
#include <functional>
#include <memory>
#include <vector>

namespace mongo {
namespace repl {

class PrimaryOnlyServiceInstance {
public:
    PrimaryOnlyServiceInstance(long long term, OpTime opTime) : _term(term), _opTime(opTime) {}
    virtual ~PrimaryOnlyServiceInstance() = default;

    /**
     * Initialize any needed initial state based on the state document provided.
     * Must not block.
     * Throws on error.
     */
    virtual void initialize(const BSONObj& state) = 0;

    /**
     * The XXXXX mechanism will call this repeatedly as long as this node remains primary in _term.
     * Can block and do disk AND network i/o.
     * Is responsible for persisting any state needed so that its work can be resumed if it is
     * killed and restarted as a new instance.  Returns the OpTime that must be visible for
     * subsequent calls to startup with a 'state' object read at that optime to resume this work.
     * Throws on error.
     */
    void runOnce(OperationContext* opCtx);

protected:
    virtual OpTime runOnceImpl(OperationContext* opCtx) = 0;

    const long long _term;

    OpTime _opTime;  // todo comment
};

// TODO how do you start a new instance on the fly?
class PrimaryOnlyServiceGroup {
private:
    using ConstructExecutorFn = std::function<std::unique_ptr<executor::TaskExecutor>()>;
    using ConstructInstanceFn =
        std::function<std::shared_ptr<PrimaryOnlyServiceInstance>(long long, OpTime)>;

public:
    PrimaryOnlyServiceGroup(NamespaceString ns,
                            ConstructInstanceFn constructInstanceFn,
                            ConstructExecutorFn constructExecutorFn);
    virtual ~PrimaryOnlyServiceGroup() = default;

    void startup(long long term);

    void shutdown();

    void startNewInstance(const BSONObj& initialState, const OpTime& initialOpTime);

private:
    void _startNewInstance(WithLock, const BSONObj& initialState, const OpTime& initialOpTime);

    void _startup(OperationContext* opCtx);

    void _taskInstanceRunner(const mongo::executor::TaskExecutor::CallbackArgs& args,
                             std::shared_ptr<PrimaryOnlyServiceInstance> instance) noexcept;

    // Namespace where docs containing state for instances of this service group are stored.
    const NamespaceString _ns;

    // Function for constructing new instances of this ServiceGroup.
    const ConstructInstanceFn _constructInstanceFn;

    // Function for constructing new a TaskExecutor for each term in which this node is primary.
    const ConstructExecutorFn _constructExecutorFn;

    // TODO MUTEX

    // 'none' when we're secondary, current term when we're primary.
    boost::optional<long long> _term;

    std::unique_ptr<executor::TaskExecutor> _executor;

    std::vector<std::shared_ptr<PrimaryOnlyServiceInstance>> _instances;
};

class PrimaryOnlyServiceRegistry {  // todo make this a ReplicaSetAwareService
public:
    PrimaryOnlyServiceRegistry() {}

    static PrimaryOnlyServiceRegistry* get(ServiceContext* serviceContext);

    /**
     * Iterates over all registered services and starts them up.
     */
    void onStepUp(long long term);

    void onStepDown();

    void registerServiceGroup(std::string serviceName,
                              std::unique_ptr<PrimaryOnlyServiceGroup> service);

    PrimaryOnlyServiceGroup* lookupService(StringData serviceName);

private:
    // todo use fast string map
    stdx::unordered_map<std::string, std::unique_ptr<PrimaryOnlyServiceGroup>> _services;
};

}  // namespace repl

}  // namespace mongo
