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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_BATCHER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_BATCHER_H_

#include <boost/container/small_vector.hpp>
#include <oxt/thread.hpp>
#include <cassert>
#include <cstddef>

#include <ev.h>

#include <Logging.h>
#include <Utils/StrIntUtils.h>
#include <Utils/JsonUtils.h>
#include <Utils/SystemTime.h>
#include <UstRouter/Transaction.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;
using namespace boost;
using namespace oxt;


class Batcher {
private:
	size_t limit;
	oxt::thread *thread;

	boost::mutex syncher;
	boost::condition_variable cond;
	TransactionList queued;
	size_t bytesQueued, bytesProcessing, peakSize;
	unsigned int nQueued, nProcessing;
	unsigned long long lastQueueAddTime, lastProcessingBeginTime, lastProcessingEndTime;
	bool quit;

	void threadMain() {
		try {
			realThreadMain();
		} catch (const thread_interrupted &) {
			break;
		} catch (const tracable_exception &e) {
			P_WARN("ERROR: " << e.what() << "\n  Backtrace:\n" << e.backtrace());
		}
	}

	void realThreadMain() {
		TRACE_POINT();
		boost::unique_lock<boost::mutex> l(syncher);

		while (true) {
			UPDATE_TRACE_POINT();
			while (!quit && STAILQ_EMPTY(&queued)) {
				cond.wait(l);
			}

			if (STAILQ_EMPTY(&queued)) {
				// We honor the quit signal only after having processed
				// everything in the queue.
				if (quit) {
					return;
				}
			} else {
				UPDATE_TRACE_POINT();
				TransactionList transactions = consumeQueue();
				l.unlock();

				UPDATE_TRACE_POINT();
				performBatching(transactions);

				UPDATE_TRACE_POINT();
				l.lock();
				clearProcessingStatistics();
			}
		}
	}

	TransactionList consumeQueue() {
		TRACE_POINT();
		TransactionList processing;
		boost::lock_guard<boost::mutex> pl(processingLock);

		P_ASSERT_EQ(bytesProcessing, 0);
		P_ASSERT_EQ(nProcessing, 0);

		bytesProcessing = bytesQueued;
		nProcessing = nQueued;
		STAILQ_SWAP(&queued, &processing, TransactionList);
		bytesQueued = 0;
		nQueued = 0;
		lastProcessingBeginTime = SystemTime::getUsec();

		return processing;
	}

	void performBatching(TransactionList &transactions) {
		TRACE_POINT();
		Transactionlist undersizedTransactions, oversizedTransactions;

		STAILQ_INIT(&undersizedTransactions);
		STAILQ_INIT(&oversizedTransactions);

		BatchingAlgorithm::organizeTransactionsBySize(transactions,
			undersizedTransactions, oversizedTransactions, striveBatchSize);
		assert(STAILQ_EMPTY(transactions));
		BatchingAlgorithm::organizeUndersizedTransactionsIntoBatches(
			undersizedTransactions, striveBatchSize);

		BatchList<16> batches;

		UPDATE_TRACE_POINT();
		BatchingAlgorithm::createBatchObjectsForUndersizedTransactions(
			undersizedTransactions, batches, striveBatchSize);

		UPDATE_TRACE_POINT();
		BatchingAlgorithm::createBatchObjectsForOversizedTransactions(
			oversizedTransactions, batches, striveBatchSize);

		UPDATE_TRACE_POINT();
		sendOutBatches(batches);
	}

	void clearProcessingStatistics() {
		bytesProcessing = 0;
		nProcessing = 0;
		lastProcessingEndTime = SystemTime::getUsec();
	}

	template<typename BatchList>
	void sendOutBatches(const BatchList &batches) {
		// TODO
	}

	string getRecommendedBufferSize() const {
		return toString(peakBufferSize * 2 / 1024) + " KB";
	}

public:
	Batcher()
		: thread(NULL),
		  quit(false)
	{
		STAILQ_INIT(&queued);
		STAILQ_INIT(&processing);
	}

	~Batcher() {
		shutdown();
	}

	void start() {
		thread = new thread(boost::bind(&Batcher::threadMain, this),
			"RemoteSink batcher", 1024 * 1024);
	}

	void shutdown() {
		boost::unique_lock<boost::mutex> l(syncher);
		quit = true;
		cond.notify();
		l.unlock();

		if (thread != NULL) {
			thread->join();
			delete thread;
			thread = NULL;
		}

		l.lock();
		assert(STAILQ_EMPTY(&queued));
	}

	bool add(TransactionList &transactions, size_t totalBodySize, unsigned int count, ev_tstamp now) {
		boost::lock_guard<boost::mutex> l(syncher);

		assert(!quit);

		if (bytesQueued + bytesProcessing > limit) {
			P_WARN("Unable to batch and compress Union Station data quickly enough. Please "
				"lower the compression level to speed things up, or increase the batching "
				"buffer's size (recommended size: " + getRecommendedBufferSize() + ")");
			return false;
		} else {
			bytesQueued += totalBodySize;
			nQueued += count;
			peakSize = std::max(peakSize, bytesQueued + bytesProcessing);
			lastQueueAddTime = now * 1000000;
			STAILQ_CONCAT(&transactions, &queued);
			cond.notify();
			return true;
		}
	}

	Json::Value inspectStateAsJson() const {
		Json::Value doc;
		boost::lock_guard<boost::mutex> l(queue);

		doc["total_size"] = byteSizeToJson(bytesQueued + bytesProcessing);
		doc["queued_size"] = byteSizeToJson(bytesQueued);
		doc["processing_size"] = byteSizeToJson(bytesProcessing);
		doc["peak_total_size"] = byteSizeToJson(peakSize);
		doc["total_count"] = nQueued + nProcessing;
		doc["queued_count"] = nQueued;
		doc["processing_count"] = nProcessing;
		doc["total_size_limit"] = limit;

		doc["last_queue_add_time"] = timeToJson(lastQueueAddTime);
		doc["last_processing_begin_time"] = timeToJson(lastProcessingBeginTime);
		doc["last_processing_end_time"] = timeToJson(lastProcessingEndTime);

		return doc;
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_BATCHER_H_ */
