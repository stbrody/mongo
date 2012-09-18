// sasl.h

/**
*    Copyright (C) 2009 10gen Inc.
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
*/

#pragma once

#include <string>

#include <gsasl.h>

namespace mongo {

    class SaslInfo : boost::noncopyable {
    public:
        SaslInfo();
        ~SaslInfo();

        static int serverCallback (Gsasl * ctx, Gsasl_session * sctx, Gsasl_property prop);

        //private:
        Gsasl* _context;
    };

    extern SaslInfo saslInfo;

    class SaslSession : boost::noncopyable {
    public:
        //        SaslSession() {}
        //        ~SaslSession() {}

        static void serverBegin(const string& dbname,
                                const string& authMechanism,
                                const string& username,
                                const string& saslData,
                                string& saslResponse,
                                string& errmsg);

        static void serverContinue(const string& saslData, string& saslResponse, string& errmsg);

        Gsasl_session* _session;

        static boost::thread_specific_ptr<SaslSession> gsaslSession;
    };

} // namespace mongo
