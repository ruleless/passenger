/*
 *  Phusion Passenger - https://www.phusionpassenger.com/
 *  Copyright (c) 2010-2016 Phusion Holding B.V.
 *
 *  "Passenger", "Phusion Passenger" and "Union Station" are registered
 *  trademarks of Phusion Holding B.V.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SENDER_SERVER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SENDER_SERVER_H_

#include <limits.h>
#include <algorithm>
#include <cstddef>
#include <boost/cstdint.hpp>
#include <StaticString.h>
#include <Utils/StrIntUtils.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSender {


class Server {
private:
	// All strings must be NULL terminated.
	boost::uint8_t hostHeaderSize, pingUrlSize, sinkUrlSize;
	char hostHeader[sizeof("Host: gateway.unionstationapp.com") + 32];
	char pingUrl[sizeof("https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:3000/sink") + 16];
	char sinkUrl[sizeof("https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:3000/sink") + 16];
	unsigned int certificatePathSize;
	char certificatePath[PATH_MAX + 1];
	const CurlProxyInfo proxyInfo;

public:
	Server(const StaticString &ip, unsigned short port,
		const StaticString &hostName,
		const StaticString &certificate = StaticString(),
		const CurlProxyInfo &_proxyInfo = CurlProxyInfo())
		: proxyInfo(_proxyInfo);
	{
		char *pos;
		const char *end;

		// Generate host header
		pos = hostHeader;
		end = hostHeader + sizeof(hostHeader) - 1;
		pos = appendData(pos, end, P_STATIC_STRING("Host: "));
		pos = appendData(pos, end, hostName);
		*pos = '\0';

		// Generate ping URL and sink URL
		pos = pingUrl;
		end = pingUrl + sizeof(pingUrl) - 1;
		if (looksLikeIPv6(ip)) {
			pos = appendData(pos, end, P_STATIC_STRING("https://["));
			pos = appendData(pos, end, ip);
			pos = appendData(pos, end, P_STATIC_STRING("]"));
		} else {
			pos = appendData(pos, end, P_STATIC_STRING("https://"));
			pos = appendData(pos, end, ip);
		}
		pos = appendData(pos, end, P_STATIC_STRING(":"));
		pos += uintToString(port, pos, end - pos);

		size_t baseUrlSize = pos - pingUrl;

		pos = appendData(pos, end, P_STATIC_STRING("/ping"));
		*pos = '\0';
		pingUrlSize = pos - pingUrl;

		pos = sinkUrl;
		end = sinkUrl + sizeof(sinkUrl) - 1;
		pos = appendData(pos, end, StaticString(pingUrl, baseUrlSize));
		pos = appendData(pos, end, P_STATIC_STRING("/sink"));
		*pos = '\0';
		sinkUrlSize = pos - sinkUrl;

		// Intern certificate path
		memcpy(certificatePath, certificate.data(), certificate.size());
		certificatePathSize = std::min<unsigned int>(PATH_MAX, certificate.size());
		certificatePath[certificatePathSize] = '\0';
	}

	// NULL terminated.
	StaticString getHostHeader() const {
		return StaticString(hostHeader, hostHeaderSize);
	}

	// NULL terminated.
	StaticString getPingUrl() const {
		return StaticString(pingUrl, pingUrlSize);
	}

	// NULL terminated.
	StaticString getSinkUrl() const {
		return StaticString(sinkUrl, sinkUrlSize);
	}

	// NULL terminated.
	StaticString getCertificatePath() const {
		return StaticString(certificatePath, certificatePathSize);
	}

	const CurlProxyInfo &getCurlProxyInfo() const {
		return proxyInfo;
	}
};


} // namespace RemoteSender
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SENDER_SERVER_H_ */
