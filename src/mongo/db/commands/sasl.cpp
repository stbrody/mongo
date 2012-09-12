// sasl.cpp

#include "mongo/pch.h"

#include "mongo/db/commands.h"
#include "mongo/db/client.h"
#include "mongo/db/sasl.h"

#include <gsasl.h>

namespace mongo {
    SaslInfo::SaslInfo() {
        int status = gsasl_init(&_context);
        uassert( 16437, str::stream() << "Cannot initialize libgsasl. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK );
    }

    SaslInfo::~SaslInfo() {
        gsasl_done(_context);
    }

    class CmdSaslAuthBegin : public Command {
    public:
        virtual LockType locktype() const { return NONE; }
        virtual void help( stringstream& help ) const {
            help << "Begin Sasl auth request";
        }
        virtual bool slaveOk() const {
            return true;
        }
        CmdSaslAuthBegin() : Command("saslBegin") {}
        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg, BSONObjBuilder& result, bool fromRepl) {
            SaslInfo sasl;
            return true;
        }
    } cmdSaslAuthBegin;

}
