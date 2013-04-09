/*    Copyright 2012 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include <string>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/base64.h"
#include "mongo/util/gsasl_session.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/hostandport.h"

#include <gsasl.h>  // Must be included after "mongo/platform/cstdint.h" because of SERVER-8086.
#include <ldap.h>
#include <sasl/sasl.h>

namespace mongo {
namespace {

    // Default log level on the client for SASL log messages.
    const int defaultSaslClientLogLevel = 4;

    const char* const saslClientLogFieldName = "clientLogLevel";

    //Gsasl* _gsaslLibraryContext = NULL;

    /**
     * Configure "*session" as a client gsasl session for authenticating on the connection
     * "*client", with the given "saslParameters".  "gsasl" and "sessionHook" are passed through
     * to GsaslSession::initializeClientSession, where they are documented.
     */
    Status configureSession(Gsasl* gsasl,
                            DBClientWithCommands* client,
                            const BSONObj& saslParameters,
                            void* sessionHook,
                            GsaslSession* session) {

        std::string mechanism;
        Status status = bsonExtractStringField(saslParameters,
                                               saslCommandMechanismFieldName,
                                               &mechanism);
        if (!status.isOK())
            return status;

        status = session->initializeClientSession(gsasl, mechanism, sessionHook);
        if (!status.isOK())
            return status;

        std::string service;
        status = bsonExtractStringFieldWithDefault(saslParameters,
                                                   saslCommandServiceNameFieldName,
                                                   saslDefaultServiceName,
                                                   &service);
        if (!status.isOK())
            return status;
        session->setProperty(GSASL_SERVICE, service);

        std::string hostname;
        status = bsonExtractStringFieldWithDefault(saslParameters,
                                                   saslCommandServiceHostnameFieldName,
                                                   HostAndPort(client->getServerAddress()).host(),
                                                   &hostname);
        if (!status.isOK())
            return status;
        session->setProperty(GSASL_HOSTNAME, hostname);

        BSONElement principalElement = saslParameters[saslCommandPrincipalFieldName];
        if (principalElement.type() == String) {
            session->setProperty(GSASL_AUTHID, principalElement.str());
        }
        else if (!principalElement.eoo()) {
            return Status(ErrorCodes::TypeMismatch,
                          str::stream() << "Expected string for " << principalElement);
        }

        BSONElement passwordElement = saslParameters[saslCommandPasswordFieldName];
        if (passwordElement.type() == String) {
            bool digest;
            status = bsonExtractBooleanFieldWithDefault(saslParameters,
                                                        saslCommandDigestPasswordFieldName,
                                                        true,
                                                        &digest);
            if (!status.isOK())
                return status;

            std::string passwordHash;
            if (digest) {
                passwordHash = client->createPasswordDigest(principalElement.str(),
                                                            passwordElement.str());
            }
            else {
                passwordHash = passwordElement.str();
            }
            session->setProperty(GSASL_PASSWORD, passwordHash);
        }
        else if (!passwordElement.eoo()) {
            return Status(ErrorCodes::TypeMismatch,
                          str::stream() << "Expected string for " << passwordElement);
        }

        return Status::OK();
    }

    int getSaslClientLogLevel(const BSONObj& saslParameters) {
        int saslLogLevel = defaultSaslClientLogLevel;
        BSONElement saslLogElement = saslParameters[saslClientLogFieldName];
        if (saslLogElement.trueValue())
            saslLogLevel = 1;
        if (saslLogElement.isNumber())
            saslLogLevel = saslLogElement.numberInt();
        return saslLogLevel;
    }

    LDAP* ld = NULL;
    MONGO_INITIALIZER(SaslClientContext)(InitializerContext* context) {
        char* ldapuri;
        char schemeString[] = "ldap";
        char hostString[] = "54.234.147.246";
        LDAPURLDesc url;
        memset( &url, 0, sizeof(url));
        url.lud_scheme = schemeString;
        url.lud_host = hostString;
        url.lud_port = 50000;
        url.lud_scope = LDAP_SCOPE_DEFAULT;
        ldapuri = ldap_url_desc2str( &url );
        int rc = ldap_initialize(&ld, ldapuri);
        if (rc != LDAP_SUCCESS) {
            return Status(ErrorCodes::UnknownError, ldap_err2string(rc));
        }
        int protocol = LDAP_VERSION3;
        if( ldap_set_option( ld, LDAP_OPT_PROTOCOL_VERSION, &protocol ) != LDAP_OPT_SUCCESS )
        {
            return Status(ErrorCodes::UnknownError, "Could not set LDAP_OPT_PROTOCOL_VERSION");
        }
        return Status::OK();
    }

    typedef struct lutil_sasl_defaults_s {
        char *mech;
        char *realm;
        char *authcid;
        char *passwd;
        char *authzid;
        char **resps;
        int nresps;
    } lutilSASLdefaults;

    int ldap_sasl_interact_func(LDAP* ld, unsigned flags, void* defaultsVoidPtr, void* in) {
        cout << "In my custom ldap_sasl_interact_func" << endl;
        lutilSASLdefaults* defaults = (lutilSASLdefaults*) defaultsVoidPtr;
        sasl_interact_t* interact = (sasl_interact_t*) in;
        if( ld == NULL ) return LDAP_PARAM_ERROR;

        while (interact->id != SASL_CB_LIST_END) {
            switch( interact->id ) {
            case SASL_CB_GETREALM:
                cout << "Asked for realm" << endl;
                interact->defresult = defaults->realm;
                break;
            case SASL_CB_AUTHNAME:
                cout << "Asked for authcid (authname)" << endl;
                interact->defresult = defaults->authcid;
                break;
            case SASL_CB_PASS:
                cout << "Asked for password" << endl;
                interact->defresult = defaults->passwd;
                break;
            case SASL_CB_USER:
                cout << "Asked for authzid (user)" << endl;
                interact->defresult = defaults->authzid;
                break;
            default:
                cout << "WAS ASKED FOR SOMETHING ELSE: " << interact->id << endl;
                break;
            }

            interact->result = (interact->defresult && *interact->defresult) ?
                    interact->defresult : "";
            interact->len = strlen(interact->defresult);

            interact++;
        }
        return LDAP_SUCCESS;
    }

    Status saslClientAuthenticateImpl(DBClientWithCommands* client,
                                      const BSONObj& saslParameters,
                                      void* sessionHook) {
        lutilSASLdefaults defaults;
        char mechanism[] = "DIGEST-MD5";
        char* realm = NULL;
        char authcid[] = "a";
        char authzid[] = "a";
        char passwd[] = "a";
        defaults.mech = mechanism;
        defaults.realm = realm;
        defaults.authcid = authcid;
        defaults.authzid = authzid;
        defaults.passwd = passwd;
        defaults.resps = NULL;
        defaults.nresps = 0;

        int rc = ldap_sasl_interactive_bind_s( ld, NULL, mechanism, NULL,
                                               NULL, 0, ldap_sasl_interact_func, (void*) &defaults );
        if (rc != LDAP_SUCCESS) {
            cout << "UHOH!!!!" << endl;
            char *msg=NULL;
            ldap_get_option( ld, LDAP_OPT_DIAGNOSTIC_MESSAGE, (void*)&msg);
            if (msg)
                cout << "diagnostic message: " << string(msg) << endl;
            return Status(ErrorCodes::UnknownError, mongoutils::str::stream() << "ERROR! Code: " <<
                          rc << ", message: " << ldap_err2string(rc));
        }
        return Status::OK();

        /*
        cout << "AAAAAAAAAAAAA" << endl;
        const char* dn = NULL; // Docs say this should always be NULL for sasl
        //const char* mechanism = LDAP_SASL_SIMPLE;
        const char* mechanism = "DIGEST-MD5";
        char passwd[] = "a";
        struct berval passwdStruct = {strlen(passwd), passwd};
        LDAPControl** sctrls = NULL;
        LDAPControl** cctrls = NULL;
        int msgid, rc;
        rc = ldap_sasl_bind(ld, dn, mechanism, &passwdStruct, sctrls, cctrls, &msgid);
        if (msgid == -1) {
            return Status(ErrorCodes::UnknownError, mongoutils::str::stream() << "ERRORERROR: " << ldap_err2string(rc));
        }
        cout << "BBBBBBBBBBBBBBBB" << endl;
        LDAPMessage *result;
        struct timeval timeout;
        timeout.tv_sec = 1;
        rc = ldap_result( ld, msgid, LDAP_MSG_ALL, &timeout, &result );
        cout <<"b.5b.5b.5b.5b.5b.5b.5b.5b.5b.5b.5b.5b.5b.5b.5" << endl;
        if (rc == 0 || rc == 1) {
            char *msg=NULL;
            ldap_get_option( ld, LDAP_OPT_DIAGNOSTIC_MESSAGE, (void*)&msg);
            cout << msg << endl;
            return Status(ErrorCodes::UnknownError, "something bad happened");
        }
        cout << "CCCCCCCCCCCCC" << endl;
        int err;
        char* matched = NULL;
        char* info = NULL;
        char** refs = NULL;
        LDAPControl** ctrls;
        rc = ldap_parse_result( ld, result, &err, &matched, &info, &refs, &ctrls, 1 );
        cout << "DDDDDDDDDDDDDD" << endl;
        if ( rc != LDAP_SUCCESS ) {
            return Status(ErrorCodes::UnknownError, mongoutils::str::stream() << "ERRORERROR: " << ldap_err2string(rc));
        } else {
            cout << "GREAT SUCCESS!" << endl;
        }
        return Status::OK();*/
        /*GsaslSession session;

        int saslLogLevel = getSaslClientLogLevel(saslParameters);

        Status status = configureSession(_gsaslLibraryContext,
                                         client,
                                         saslParameters,
                                         sessionHook,
                                         &session);
        if (!status.isOK())
            return status;

        std::string targetDatabase;
        status = bsonExtractStringFieldWithDefault(saslParameters,
                                                   saslCommandPrincipalSourceFieldName,
                                                   saslDefaultDBName,
                                                   &targetDatabase);
        if (!status.isOK())
            return status;

        BSONObj saslFirstCommandPrefix = BSON(
                saslStartCommandName << 1 <<
                saslCommandMechanismFieldName << session.getMechanism());

        BSONObj saslFollowupCommandPrefix = BSON(saslContinueCommandName << 1);
        BSONObj saslCommandPrefix = saslFirstCommandPrefix;
        BSONObj inputObj = BSON(saslCommandPayloadFieldName << "");
        bool isServerDone = false;
        while (!session.isDone()) {
            std::string payload;
            BSONType type;

            status = saslExtractPayload(inputObj, &payload, &type);
            if (!status.isOK())
                return status;

            LOG(saslLogLevel) << "sasl client input: " << base64::encode(payload) << endl;

            std::string responsePayload;
            status = session.step(payload, &responsePayload);
            if (!status.isOK())
                return status;

            LOG(saslLogLevel) << "sasl client output: " << base64::encode(responsePayload) << endl;

            BSONObjBuilder commandBuilder;
            commandBuilder.appendElements(saslCommandPrefix);
            commandBuilder.appendBinData(saslCommandPayloadFieldName,
                                         int(responsePayload.size()),
                                         BinDataGeneral,
                                         responsePayload.c_str());
            BSONElement conversationId = inputObj[saslCommandConversationIdFieldName];
            if (!conversationId.eoo())
                commandBuilder.append(conversationId);

            // Server versions 2.3.2 and earlier may return "ok: 1" with a non-zero "code" field,
            // indicating a failure.  Subsequent versions should return "ok: 0" on failure with a
            // non-zero "code" field to indicate specific failure.  In all versions, ok: 1, code: >0
            // and ok: 0, code optional, indicate failure.
            bool ok = client->runCommand(targetDatabase, commandBuilder.obj(), inputObj);
            ErrorCodes::Error code = ErrorCodes::fromInt(
                    inputObj[saslCommandCodeFieldName].numberInt());

            if (!ok || code != ErrorCodes::OK) {
                if (code == ErrorCodes::OK)
                    code = ErrorCodes::UnknownError;

                return Status(code, inputObj[saslCommandErrmsgFieldName].str());
            }

            isServerDone = inputObj[saslCommandDoneFieldName].trueValue();
            saslCommandPrefix = saslFollowupCommandPrefix;
        }

        if (!isServerDone)
            return Status(ErrorCodes::ProtocolError, "Client finished before server.");
        return Status::OK();*/
    }

    MONGO_INITIALIZER(SaslClientAuthenticateFunction)(InitializerContext* context) {
        saslClientAuthenticate = saslClientAuthenticateImpl;
        return Status::OK();
    }

}  // namespace
}  // namespace mongo
