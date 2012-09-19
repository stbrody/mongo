// sasl.cpp

#include "mongo/pch.h"

#include "mongo/db/commands.h"
#include "mongo/db/sasl.h"

#include <gsasl.h>

namespace mongo {

    class CmdSaslAuthBegin : public Command {
    public:
        virtual LockType locktype() const { return NONE; }
        virtual void help( stringstream& help ) const {
            help << "Begin Sasl auth request";
        }
        virtual bool slaveOk() const {
            return true;
        }
        virtual bool requiresAuth() {
            return false;
        }
        CmdSaslAuthBegin() : Command("saslBegin") {}
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            const string& authMechanism = cmdObj["mechanism"].String();
            log() << "received mechanism: " << authMechanism << "." << endl;

            const string& username = cmdObj["username"].String(); // TODO: get username from SASL

            const string& saslData = cmdObj["saslData"].String();

            string saslResponse;
            try {
                SaslSession::serverBegin(dbname, authMechanism, username, saslData, saslResponse, errmsg);
            } catch (DBException e) {
                log() << "Error occurred! " << e << endl;
                throw;
            }

            result.append("saslResponse", saslResponse);

            return true;
        }
    } cmdSaslAuthBegin;

    class CmdSaslAuthContinue : public Command {
    public:
        virtual LockType locktype() const { return NONE; }
        virtual void help( stringstream& help ) const {
            help << "Begin Sasl auth request";
        }
        virtual bool slaveOk() const {
            return true;
        }
        virtual bool requiresAuth() {
            return false;
        }
        CmdSaslAuthContinue() : Command("saslContinue") {}
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {

            const string& saslData = cmdObj["saslData"].String();

            string saslResponse;
            try {
                SaslSession::serverContinue(saslData, saslResponse, errmsg);
            } catch (DBException e) {
                log() << "Error occurred! " << e << endl;
                throw;
            }

            result.append("saslResponse", saslResponse);

            return true;
        }
    } cmdSaslAuthContinue;

}
