// sasl.cpp

#include "mongo/pch.h"

#include "mongo/db/commands.h"
#include "mongo/db/client.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/sasl.h"

#include <gsasl.h>

namespace mongo {

    SaslInfo saslInfo;
    boost::thread_specific_ptr<SaslSession> SaslSession::gsaslSession;

    SaslInfo::SaslInfo() {
        int status = gsasl_init(&_context);
        uassert( 16445, str::stream() << "Cannot initialize libgsasl. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK );
    }

    SaslInfo::~SaslInfo() {
        gsasl_done(_context);
    }

    bool DBClientWithCommands::saslAuth(const string& dbname,
                                        const string& username,
                                        const string& password_text,
                                        string& errmsg,
                                        Auth::Level * level) {
        log() << "Running saslAuth!" << endl;

        string password = createPasswordDigest( username , password_text );

        SaslSession* sessionInfo = SaslSession::gsaslSession.get();
        if ( sessionInfo == NULL ) {
            sessionInfo = new SaslSession();
            SaslSession::gsaslSession.reset( sessionInfo );
        }

        int status = gsasl_client_start( saslInfo._context, "CRAM-MD5", &sessionInfo->_session );
        uassert( 16446, str::stream() << "Cannot initialize gsasl session. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK );

        Gsasl_session* session = sessionInfo->_session;

        BSONObjBuilder beginCmd;
        beginCmd << "saslBegin" << 1;
        beginCmd << "mechanism" << "CRAM-MD5";
        beginCmd << "username" << username;

        gsasl_property_set( session, GSASL_AUTHID, username.c_str() ); // TODO: use a callback
        gsasl_property_set( session, GSASL_PASSWORD, password.c_str() ); // TODO: use a callback

        char* saslMessage;
        status = gsasl_step64( session, NULL, &saslMessage );
        uassert( 16447, str::stream() << "First step of auth failed. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK || status == GSASL_NEEDS_MORE );

        log() << "Status of first client step: " << gsasl_strerror(status) << endl;

        log() << "Sasl data to send: " << saslMessage << ". length: " << string(saslMessage).length() << endl;

        beginCmd << "saslData" << saslMessage;

        gsasl_free(saslMessage);
        saslMessage = NULL;

        BSONObj info;
        bool success = runCommand( dbname, beginCmd.done(), info );

        while ( success && status == GSASL_NEEDS_MORE ) {
            string saslResponse = info["saslResponse"].String();
            log() << "Sasl data received from server: " << saslResponse << endl;

            status = gsasl_step64( session, saslResponse.c_str(), &saslMessage );
            uassert( 16448, str::stream() << "Second step of auth failed. Code: " << status << ", message: "
                     << gsasl_strerror(status), status == GSASL_OK || status == GSASL_NEEDS_MORE );

            BSONObjBuilder continueCmd;
            continueCmd << "saslContinue" << 1;
            continueCmd << "saslData" << saslMessage;

            log() << "Sasl data to send for step 2: " << saslMessage << ". length: " << string(saslMessage).length() << endl;

            gsasl_free(saslMessage);

            BSONObj objToSend = continueCmd.done();
            log() << "Gonna send: " << objToSend << endl;
            success = runCommand( dbname, objToSend, info );
        }
        if( success && status == GSASL_OK ) {
            log() << "Auth success!" << endl;
            gsasl_finish(session); // TODO: might need to be moved into later stage. Also, what happens on errors?
            return true;
        } else {
            errmsg = info.toString();
            log() << "Auth failure!!! " << errmsg << endl;
            gsasl_finish(session); // TODO: might need to be moved into later stage. Also, what happens on errors?
            return false;
        }

        return false;
    }

}
