/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#include <asio.hpp>
#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>

#include "mongo/base/status.h"
#include "mongo/client/connection_pool.h"
#include "mongo/executor/network_interface.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/remote_command_response.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/rpc/protocol.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/net/message.h"

namespace mongo {
namespace executor {

/**
 * Implementation of the replication system's network interface using Christopher
 * Kohlhoff's ASIO library instead of existing MongoDB networking primitives.
 */
class NetworkInterfaceASIO final : public NetworkInterface {
public:
    NetworkInterfaceASIO();
    std::string getDiagnosticString() override;
    std::string getHostName() override;
    void startup() override;
    void shutdown() override;
    void waitForWork() override;
    void waitForWorkUntil(Date_t when) override;
    void signalWorkAvailable() override;
    Date_t now() override;
    void startCommand(const TaskExecutor::CallbackHandle& cbHandle,
                      const RemoteCommandRequest& request,
                      const RemoteCommandCompletionFn& onFinish) override;
    void cancelCommand(const TaskExecutor::CallbackHandle& cbHandle) override;
    void setAlarm(Date_t when, const stdx::function<void()>& action) override;

    bool inShutdown() const;

private:
    using ResponseStatus = TaskExecutor::ResponseStatus;
    using NetworkInterface::RemoteCommandCompletionFn;
    using NetworkOpHandler = stdx::function<void(std::error_code, size_t)>;

    enum class State { kReady, kRunning, kShutdown };

    /**
     * AsyncConnection encapsulates the per-connection state we maintain.
     */
    class AsyncConnection {
    public:
        AsyncConnection(asio::ip::tcp::socket&& sock, rpc::ProtocolSet serverProtocols);

        AsyncConnection(asio::ip::tcp::socket&& sock,
                        rpc::ProtocolSet serverProtocols,
                        boost::optional<ConnectionPool::ConnectionPtr>&& bootstrapConn);

        asio::ip::tcp::socket& sock();

        rpc::ProtocolSet serverProtocols() const;
        rpc::ProtocolSet clientProtocols() const;
        void setServerProtocols(rpc::ProtocolSet protocols);

// Explicit move construction and assignment to support MSVC
#if defined(_MSC_VER) && _MSC_VER < 1900
        AsyncConnection(AsyncConnection&&);
        AsyncConnection& operator=(AsyncConnection&&);
#else
        AsyncConnection(AsyncConnection&&) = default;
        AsyncConnection& operator=(AsyncConnection&&) = default;
#endif

    private:
        asio::ip::tcp::socket _sock;

        rpc::ProtocolSet _serverProtocols;
        rpc::ProtocolSet _clientProtocols{rpc::supports::kAll};

        /**
         * The bootstrap connection we use to run auth. This will eventually go away when we finish
         * implementing async auth, but for now we need to keep it alive so that the socket it
         * creates stays open.
         */
        boost::optional<ConnectionPool::ConnectionPtr> _bootstrapConn;
    };

    /**
     * AsyncCommand holds state for a currently running or soon-to-be-run command.
     */
    class AsyncCommand {
    public:
        AsyncCommand(AsyncConnection* conn);

        // This method resets the Messages and associated information held inside
        // an AsyncCommand so that it may be reused to run a new network roundtrip.
        void reset();

        NetworkInterfaceASIO::AsyncConnection& conn();

        Message& toSend();
        void setToSend(Message&& message);

        Message& toRecv();
        MSGHEADER::Value& header();

    private:
        NetworkInterfaceASIO::AsyncConnection* const _conn;

        Message _toSend;
        Message _toRecv;

        // TODO: Investigate efficiency of storing header separately.
        MSGHEADER::Value _header;
    };

    /**
     * Helper object to manage individual network operations.
     */
    class AsyncOp {
    public:
        AsyncOp(const TaskExecutor::CallbackHandle& cbHandle,
                const RemoteCommandRequest& request,
                const RemoteCommandCompletionFn& onFinish,
                Date_t now);

        std::string toString() const;

        void cancel();
        bool canceled() const;

        const TaskExecutor::CallbackHandle& cbHandle() const;

        AsyncConnection* connection();

        void connect(ConnectionPool* const pool, asio::io_service* service, Date_t now);
        void setConnection(AsyncConnection&& conn);
        bool connected() const;

        // AsyncOp may run multiple commands over its lifetime (for example, an ismaster
        // command, the command provided to the NetworkInterface via startCommand(), etc.)
        // Calling beginCommand() resets internal state to prepare to run newCommand.
        AsyncCommand& beginCommand(Message&& newCommand);
        AsyncCommand& command();

        void finish(const TaskExecutor::ResponseStatus& status);

        const RemoteCommandRequest& request() const;

        Date_t start() const;

        rpc::Protocol operationProtocol() const;

        void setOperationProtocol(rpc::Protocol proto);

    private:
        enum class OpState {
            kReady,
            kConnectionAcquired,
            kConnectionVerified,
            kConnected,
            kCompleted
        };

        // Information describing a task enqueued on the NetworkInterface
        // via a call to startCommand().
        TaskExecutor::CallbackHandle _cbHandle;
        RemoteCommandRequest _request;
        RemoteCommandCompletionFn _onFinish;

        /**
         * The connection state used to service this request. We wrap it in an optional
         * as it is instantiated at some point after the AsyncOp is created.
         */
        boost::optional<AsyncConnection> _connection;

        /**
         * The RPC protocol used for this operation. We wrap it in an optional as it
         * is not known until we obtain a connection.
         */
        boost::optional<rpc::Protocol> _operationProtocol;

        const Date_t _start;

        OpState _state;
        AtomicUInt64 _canceled;

        /**
         * An AsyncOp may run 0, 1, or multiple commands over its lifetime.
         * AsyncOp only holds at most a single AsyncCommand object at a time,
         * representing its current running or next-to-be-run command, if there is one.
         */
        boost::optional<AsyncCommand> _command;
    };

    void _startCommand(AsyncOp* op);

    /**
     * Wraps a completion handler in pre-condition checks.
     * When we resume after an asynchronous call, we may find the following:
     *    - the AsyncOp has been canceled in the interim (via cancelCommand())
     *    - the asynchronous call has returned a non-OK error code
     * Should both conditions be present, we handle cancelation over errors. States use
     * _validateAndRun() to perform these checks before advancing the state machine.
     */
    template <typename Handler>
    void _validateAndRun(AsyncOp* op, std::error_code ec, Handler&& handler) {
        if (op->canceled())
            return _completeOperation(op,
                                      Status(ErrorCodes::CallbackCanceled, "Callback canceled"));
        if (ec)
            return _networkErrorCallback(op, ec);

        handler();
    }

    std::unique_ptr<Message> _messageFromRequest(const RemoteCommandRequest& request,
                                                 rpc::Protocol protocol);

    // Connection
    void _connectASIO(AsyncOp* op);
    void _connectWithDBClientConnection(AsyncOp* op);
    void _setupSocket(AsyncOp* op, const asio::ip::tcp::resolver::iterator& endpoints);
    void _runIsMaster(AsyncOp* op);
    void _authenticate(AsyncOp* op);
    void _sslHandshake(AsyncOp* op);

    // Communication state machine
    void _beginCommunication(AsyncOp* op);
    void _completedOpCallback(AsyncOp* op);
    void _networkErrorCallback(AsyncOp* op, const std::error_code& ec);
    void _completeOperation(AsyncOp* op, const TaskExecutor::ResponseStatus& resp);

    void _signalWorkAvailable_inlock();

    void _asyncRunCommand(AsyncCommand* cmd, NetworkOpHandler handler);

    asio::io_service _io_service;
    stdx::thread _serviceRunner;

    asio::ip::tcp::resolver _resolver;

    std::atomic<State> _state;

    stdx::mutex _inProgressMutex;
    std::unordered_map<AsyncOp*, std::unique_ptr<AsyncOp>> _inProgress;

    stdx::mutex _executorMutex;
    bool _isExecutorRunnable;
    stdx::condition_variable _isExecutorRunnableCondition;

    std::unique_ptr<ConnectionPool> _connPool;
};

}  // namespace executor
}  // namespace mongo
