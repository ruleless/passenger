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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SEGMENT_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SEGMENT_H_

#include <boost/container/small_vector.hpp>
#include <cstddef>
#include <psg_sysqueue.h>
#include <UstRouter/Transaction.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;


struct Segment {
	typedef boost::container::small_vector<4, ServerPtr> SmallServerList;

	/****** Fields used by Segmenter ******/

	STAILQ_ENTRY(Segment) nextInSegmenterList;
	string segmentKey;
	bool scheduledForBatching;


	/****** Fields used by Segmenter and Batcher ******/

	// Linked list of all segments that a Batcher::add() call
	// should process.
	STAILQ_ENTRY(Segment) nextScheduledForBatching;

	// List of transactions, provided by the Segmenter, to
	// be batched by the Batcher.
	TransactionList incomingTransactions;
	size_t bytesIncomingTransactions;
	unsigned int nIncomingTransactions;


	/****** Fields used by Batcher ******/

	TransactionList queue;
	size_t bytesAdded;
	size_t bytesQueued;
	size_t bytesProcessing;
	unsigned long long lastQueueAddTime;
	unsigned long long lastProcessingBeginTime;
	unsigned long long lastProcessingEndTime;
	unsigned int nQueued;
	unsigned int nProcessing;


	/****** Fields used by Segmenter and Sender ******/

	SmallServerList servers;


	/****** Fields used by Sender *******/

	SmallServerList balancingList;
	unsigned int nextServer;
	bool allHealthy;
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SEGMENT_H_ */
