// sasl.cpp

#include "mongo/pch.h"

#include "mongo/db/commands.h"
#include "mongo/db/client.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/sasl.h"

#include <gsasl.h>

namespace mongo {

    int SaslInfo::serverCallback (Gsasl * context, Gsasl_session * session, Gsasl_property property) {
        int rc = GSASL_NO_CALLBACK;

        const char* username = gsasl_property_fast (session, GSASL_AUTHID);
        BSONObj userObj;
        string password;
        CmdAuthenticate::getUserObj(SaslSession::gsaslSession->dbname, username, userObj, password);

        switch (property) {
        case GSASL_PASSWORD:
            gsasl_property_set (session, property, password.c_str());
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
            const char* username = gsasl_property_fast (session, GSASL_AUTHID);
            // TODO: should actually get read-only value from user obj.
            CmdAuthenticate::authenticate( SaslSession::gsaslSession->dbname, username, false );
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

        SaslSession::gsaslSession->dbname = dbname;
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

}
