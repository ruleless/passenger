/*
 *  Phusion Passenger - https://www.phusionpassenger.com/
 *  Copyright (c) 2016 Phusion Holding B.V.
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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_

#include <boost/thread.hpp>
#include <vector>

#include <DataStructures/StringKeyTable.h>
#include <UstRouter/RemoteSink/Server.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;
using namespace boost;


class ServerDatabase {
public:
	class Observer {
		virtual ~Observer() { }
		virtual serverDefinitionCheckedOut(const vector< pair<ServerDefinitionPtr *, void *> > &result) = 0;
	};

	struct KeyInfo {
		string groupId;
		unsigned long long lastCheckTime;
		unsigned long long lastRejectionErrorTime;
		unsigned long long recheckTimeoutWhenAllHealthy;
		unsigned long long recheckTimeoutWhenHaveErrors;
		bool allServersHealthy;

		KeyInfo()
			: lastCheckTime(0),
			  lastRejectionErrorTime(0),
			  recheckTimeoutWhenAllHealthy(5 * 60 * 1000000),
			  recheckTimeoutWhenHaveErrors(60 * 1000000),
			  allServersHealthy(true)
			{ }
	};

private:
	typedef boost::container::small_vector<4, ServerPtr> SmallServerList;
	typedef vector<ServerPtr> ServerList;

	struct Group {
		SmallServerList servers;
		SmallServerList balancingList;
		bool allHealthy;

		Group()
			: allHealthy(true)
			{ }
	};

	boost::mutex syncher;
	StringKeyTable<KeyInfo> keys;
	StringKeyTable<Group> groups;
	StringKeyTable<bool> queue;

public:
	template<typename Container>
	bool groupKeys(StaticString keys, Container &groupIds) {

	}

	bool getGroupIdsForKeys(const HashedStaticString keys[], unsigned int count,
		GetGroupIdsForKeysCallback callback, void *userData)
	{
		boost::lock_guard<boost::mutex> l(syncher);
		for (unsigned int i = 0; i < count; i++) {
			KeyInfo *keyInfo;

			if (keys.lookup(key, &keyInfo)) {
				groupIds.push_back(keyInfo->groupId);
			} else {
				break;
			}
		}
		if (groupIds.size() == count) {
			return true;
		} else {
			for (unsigned int = 0; i < count; i++) {
				queuedGroupIdToKeyLookups.insert(keys[i], true, false);
			}
			queuedGroupIdToKeyLookupCallbacks.push_back(keys);
			wakeupEventLoop();
			return false;
		}
	}

	ServerPtr getNextUpServer(const StaticString &groupId) {
		boost::lock_guard<boost::mutex> l(syncher);
		KeyInfo *keyInfo;

		if (keys.lookup(key, &keyInfo)) {
			return checkoutFromGroup(keyInfo->groupId);
		} else {
			queue.insert(key, true, false);
			wakeupEventLoop();
			return CheckoutResult(true, ServerPtr());
		}
	}

	void reportRequestRejected(const HashedStaticString &key, size_t uploadSize,
		unsigned long long uploadTime, const string &errorMessage)
	{
		boost::lock_guard<boost::mutex> l(syncher);
		KeyInfo *keyInfo;

		if (keys.lookup(key, &keyInfo)) {
			unsigned long long now = SystemTime::getUsec();
			keyInfo->lastRejectionErrorTime = now;
			server->reportRequestRejected(uploadSize, uploadTime, errorMessage, now);
			recreateBalancingList();
		}
	}

	void reportRequestDropped(const ServerPtr &server, size_t uploadSize,
		const string &errorMessage)
	{
		server->reportRequestDropped(uploadSize, errorMessage);
		boost::lock_guard<boost::mutex> l(syncher);
		recreateBalancingList();
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SERVER_DATABASE_H_ */
