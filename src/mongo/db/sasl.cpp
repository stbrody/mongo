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
        uassert( 16437, str::stream() << "Cannot initialize libgsasl. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK );
    }

    SaslInfo::~SaslInfo() {
        gsasl_done(_context);
    }

    int SaslInfo::serverCallback (Gsasl * context, Gsasl_session * session, Gsasl_property property) {
        int rc = GSASL_NO_CALLBACK;

        string password_text = "password";
        const char* username = gsasl_property_fast (session, GSASL_AUTHID);
        string password = DBClientWithCommands::createPasswordDigest( username , password_text );

        switch (property) {
        case GSASL_PASSWORD:
            gsasl_property_set (session, property, password.c_str()); // TODO: look up proper password
            rc = GSASL_OK;
            break;

        default:
            /* You may want to log (at debug verbosity level) that an
               unknown property was requested here, possibly after filtering
               known rejected property requests. */
            warning() << "Unrecognized gsasl property requested: " << (int) property << endl;
            break;
        }

        return rc;
    }

    void SaslSession::serverContinue( const string& saslData, string& saslResponse, string& errmsg ) {

        SaslSession* sessionInfo = SaslSession::gsaslSession.get();
        if ( sessionInfo == NULL ) {
            warning() << "In saslContinue but don't have a session!" << endl;
            sessionInfo = new SaslSession();
            SaslSession::gsaslSession.reset( sessionInfo );
        }
        Gsasl_session* session = sessionInfo->_session;

        log() << "Sasl data received in step 2: " << saslData << endl;

        log() << "session: " << session << endl;
        char* stuff;
        int status = gsasl_step64( session, saslData.c_str(), &stuff );
        uassert( 16444, str::stream() << "Second step of auth failed. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK || status == GSASL_NEEDS_MORE );
        log() << "Status of second server step: " << gsasl_strerror(status) << endl;

        log() << "Sasl data to send from server from step 2: " << stuff << endl;

        saslResponse = stuff;

        gsasl_free(stuff);


        if ( status == GSASL_OK ) {
            gsasl_finish(session);
            log() << "Auth success!" << endl;
        }
    }

    void SaslSession::serverBegin(const string& dbname,
                                  const string& authMechanism,
                                  const string& username,
                                  const string& saslData,
                                  string& saslResponse,
                                  string& errmsg) {

        SaslSession* sessionInfo = SaslSession::gsaslSession.get();
        if ( sessionInfo == NULL ) {
            sessionInfo = new SaslSession();
            SaslSession::gsaslSession.reset( sessionInfo );
        }


        log() << "about to init server session with authMechanism: " << authMechanism << endl;
        int status = gsasl_server_start( saslInfo._context, authMechanism.c_str(), &sessionInfo->_session );
        uassert( 16438, str::stream() << "Cannot initialize gsasl session. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK );

        Gsasl_session* session = sessionInfo->_session;
        gsasl_property_set( session, GSASL_AUTHID, username.c_str() ); // TODO: get username from SASL

        gsasl_callback_set( saslInfo._context, SaslInfo::serverCallback );

        log() << "Sasl data received: " << saslData << endl;

        char* stuff;
        status = gsasl_step64( session, saslData.c_str(), &stuff );
        uassert( 16439, str::stream() << "First step of auth failed. Code: " << status << ", message: "
                 << gsasl_strerror(status), status == GSASL_OK || status == GSASL_NEEDS_MORE );
        log() << "Status of first server step: " << gsasl_strerror(status) << endl;

        log() << "Sasl data to send from server: " << stuff << endl;

        saslResponse = stuff;

        gsasl_free(stuff);
    }

    // Client
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
        uassert( 16441, str::stream() << "Cannot initialize gsasl session. Code: " << status << ", message: "
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
        uassert( 16442, str::stream() << "First step of auth failed. Code: " << status << ", message: "
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
            uassert( 16443, str::stream() << "Second step of auth failed. Code: " << status << ", message: "
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
