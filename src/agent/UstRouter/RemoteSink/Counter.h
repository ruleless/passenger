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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SENDER_COUNTER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SENDER_COUNTER_H_

#include <boost/cstdint.hpp>
#include <cstddef>
#include <cassert>

#include <jsoncpp/json.h>
#include <StaticString.h>
#include <Utils/SystemTime.h>
#include <Utils/JsonUtils.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSender {

using namespace std;
using namespace boost;


class BasicCounter {
private:
	unsigned long long lastActivity;
	unsigned int count;

public:
	BasicCounter()
		: lastActivity(0),
		  count(0)
		{ }

	void increment(unsigned long long now = 0) {
		update(count + 1, now);
	}

	void decrement(unsigned long long now = 0) {
		assert(count > 0);
		update(count - 1, now);
	}

	void update(unsigned int value, unsigned long long now = 0) {
		if (now == 0) {
			lastActivity = SystemTime::getUsec();
		} else {
			lastActivity = now;
		}
		count = value;
	}

	Json::Value inspectAsJson(unsigned long long now = 0) const {
		Json::Value doc;

		doc["last_activity"] = timeToJson(lastActivity, now);
		doc["count"] = count;

		return doc;
	}
};


class Counter: private BasicCounter {
private:
	boost::uint8_t lastActorNameSize;
	char lastActorName[35];

public:
	Counter()
		: BasicCounter(),
		  lastActorNameSize(0)
		{ }

	void increment(const StaticString &actorName,
		unsigned long long now = 0)
	{
		update(count + 1, actorName, now);
	}

	void decrement(const StaticString &actorName,
		unsigned long long now = 0)
	{
		assert(count > 0);
		update(count - 1, actorName, now);
	}

	void update(unsigned int value, const StaticString &actorName,
		unsigned long long now = 0)
	{
		BasicCounter::update(value, now);
		lastActorNameSize = (boost::uint8_t) std::min<size_t>(
			actorName.size(), sizeof(lastActorName));
		memcpy(lastActorName, actorName.data(), lastActorNameSize);
	}

	Json::Value inspectAsJson(unsigned long long now = 0) const {
		Json::Value doc = BasicCounter::inspectAsJson(now);
		doc["last_actor"] = StaticString(lastActorName,
			lastActorNameSize).toString();
		return doc;
	}
};


} // namespace RemoteSender
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SENDER_COUNTER_H_ */
