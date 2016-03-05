#include "TestSupport.h"
#include <SafeLibev.h>
#include <BackgroundEventLoop.h>
#include <UstRouter/RemoteSink/Segmenter.h>

using namespace Passenger;
using namespace Passenger::UstRouter;
using namespace Passenger::UstRouter::RemoteSink;
using namespace std;

namespace tut {
	struct UstRouter_RemoteSink_SegmenterTest {
		class TestBatcher: public AbstractBatcher {
		public:
			virtual void schedule(SegmentList &segments) {
				// TODO
			}
		};

		class TestSegmenter: public Segmenter {
		protected:
			virtual bool initiateApiLookup(const KeyInfoPtr &keyInfo) {
				keyInfo->lookingUp = true;
				apiLookupsInitiated.push_back(keyInfo->key);
				return true;
			}

		public:
			vector<string> apiLookupsInitiated;

			TestSegmenter(struct ev_loop *loop, AbstractBatcher *batcher,
				const VariantMap &options)
				: Segmenter(loop, batcher, options)
				{ }
		};

		BackgroundEventLoop bg;
		TransactionList transactions;
		size_t totalBodySize;
		size_t bytesAdded;
		unsigned int nAdded;
		unsigned int nTransactions;
		VariantMap options;
		TestBatcher batcher;
		TestSegmenter *segmenter;

		UstRouter_RemoteSink_SegmenterTest()
			: bg(false, true)
		{
			STAILQ_INIT(&transactions);
			totalBodySize = 0;
			bytesAdded = 0;
			nAdded = 0;
			nTransactions = 0;
			segmenter = NULL;
			options.setULL("union_station_segmenter_buffer_limit", 1024);
		}

		~UstRouter_RemoteSink_SegmenterTest() {
			Transaction *transaction, *nextTransaction;

			STAILQ_FOREACH_SAFE(transaction, &transactions, next, nextTransaction) {
				delete transaction;
			}

			delete segmenter;

			setLogLevel(DEFAULT_LOG_LEVEL);
		}

		void init() {
			segmenter = new TestSegmenter(bg.safe->getLoop(), &batcher, options);
		}

		void createTxn(const string &key) {
			Transaction *txn = new Transaction("", "", "", key, 0);
			txn->append("body");
			STAILQ_INSERT_TAIL(&transactions, txn, next);
			totalBodySize += txn->getBody().size();
			nTransactions++;
		}
	};

	DEFINE_TEST_GROUP(UstRouter_RemoteSink_SegmenterTest);

	/***** Scheduling *****/

	TEST_METHOD(1) {
		set_test_name("It queues transactions with unknown keys and initiates API lookups for those keys");

		createTxn("key1");
		createTxn("key1");
		createTxn("key2");
		createTxn("key3");
		init();
		segmenter->schedule(transactions, totalBodySize, nTransactions, bytesAdded, nAdded);
		ensure_equals("(1)", bytesAdded, totalBodySize);
		ensure_equals("(2)", nAdded, nTransactions);
		ensure("(3)", STAILQ_EMPTY(&transactions));

		ensure_equals("3 API lookups initiated",
			segmenter->apiLookupsInitiated.size(), 3u);
		ensure_equals("API lookup for key1 initiated",
			segmenter->apiLookupsInitiated[0], "key1");
		ensure_equals("API lookup for key2 initiated",
			segmenter->apiLookupsInitiated[1], "key2");
		ensure_equals("API lookup for key3 initiated",
			segmenter->apiLookupsInitiated[2], "key3");

		Json::Value doc = segmenter->inspectStateAsJson();
		ensure_equals("4 transactions queued", doc["queued"]["count"].asUInt(), 4u);
	}

	TEST_METHOD(2) {
		set_test_name("It segments transactions with known keys and forwards them to the batcher");
		set_test_name("If multiple transactions map to the same segment, then that segment is forwarded to the batcher only once");
		set_test_name("If the buffer limit has been passed then it drops the transactions");
		set_test_name("If API lookup fails to initiate then it tries again later");
	}


	/***** Next key info refresh time scheduling *****/

	TEST_METHOD(20) {
		set_test_name("It is initially not scheduled");
		set_test_name("It picks the earliest time and restarts the timer");
		set_test_name("It ignores key infos for which an API lookup is active");
		set_test_name("It stops the timer if there are no key infos");
		set_test_name("It stops the timer if all key infos are busy with API lookups");
	}


	/***** Key info refresh handling *****/

	TEST_METHOD(30) {
		set_test_name("It performs API lookups for all keys that are not already doing that");
		set_test_name("It reschedules an API lookup for a later time if initiating the lookup failed");
	}


	/***** Handling API lookup results for unknown keys *****/

	TEST_METHOD(40) {
		set_test_name("If CURL returned with an error then it drops all queued transactions with the same key");
		set_test_name("If CURL returned with an error then it logs the error");
		set_test_name("If CURL returned with an error then it schedules a refresh in the near future according to a default 'has_errors' timeout");

		set_test_name("If the response is gibberish then it drops all queued transactions with the same key");
		set_test_name("If the response is gibberish then it logs the error");
		set_test_name("If the response is gibberish then it schedules a refresh in the near future according to a default 'has_errors' timeout");

		set_test_name("If the response is parseable but not valid then it drops all queued transactions with the same key");
		set_test_name("If the response is parseable but not valid then it logs the error");
		set_test_name("If the response is parseable but not valid then it schedules a refresh in the near future according to a default 'has_errors' timeout");

		set_test_name("If the response is valid but has a non-ok status then it drops all queued transactions with the same key");
		set_test_name("If the response is valid but has a non-ok status then it logs the error");
		set_test_name("If the response is valid but has a non-ok status then it schedules a refresh according to the timeout provided by the retry_in key and updates the 'has_error' timeout");
		set_test_name("If the response is valid but has a non-ok status, and there is no retry_in key, then it schedules a refresh in the near future according to the default 'has_errors' timeout");

		set_test_name("If the response is valid but has a non-200 HTTP code then it drops all queued transactions with the same key");
		set_test_name("If the response is valid but has a non-200 HTTP code then it logs the error");
		set_test_name("If the response is valid but has a non-200 HTTP code then it schedules a refresh according to the timeout provided by the retry_in key and updates the 'has_error' timeout");
		set_test_name("If the response is valid but has a non-200 HTTP code, and there is no retry_in key, then it schedules a refresh in the near future according to the 'has_errors' timeout");

		set_test_name("If the response is valid then it forwards all queued transactions with the same key to the batcher");
		set_test_name("If the response is valid then updates the segment's server list");
		set_test_name("If the response is valid then updates the segment's timeouts");
		set_test_name("If the response is valid then it schedules a refresh in the near future according to the 'all_healthy' timeout");
	}


	/***** Handling API lookup results for known keys *****/

	TEST_METHOD(50) {
		set_test_name("If CURL returned with an error then it does not drop any queued transactions");
		set_test_name("If CURL returned with an error then it logs the error");
		set_test_name("If CURL returned with an error then it schedules a refresh in the near future according to the last 'has_errors' timeout");

		set_test_name("If the response is gibberish then it does not drop any queued transactions");
		set_test_name("If the response is gibberish then it logs the error");
		set_test_name("If the response is gibberish then it schedules a refresh in the near future according to the last 'has_errors' timeout");

		set_test_name("If the response is parseable but not valid then it does not drop any queued transactions");
		set_test_name("If the response is parseable but not valid then it logs the error");
		set_test_name("If the response is parseable but not valid then it schedules a refresh in the near future according to the last 'has_errors' timeout");

		set_test_name("If the response is valid but has a non-ok status then it does not drop any queued transactions");
		set_test_name("If the response is valid but has a non-ok status then it logs the error");
		set_test_name("If the response is valid but has a non-ok status then it schedules a refresh according to the timeout provided by the retry_in key and updates the 'has_error' timeout");
		set_test_name("If the response is valid but has a non-ok status, and there is no retry_in key, then it schedules a refresh in the near future according to the last 'has_errors' timeout");

		set_test_name("If the response is valid but has a non-200 HTTP code then it does not drop any queued transactions");
		set_test_name("If the response is valid but has a non-200 HTTP code then it logs the error");
		set_test_name("If the response is valid but has a non-200 HTTP code then it schedules a refresh according to the timeout provided by the retry_in key and updates the 'has_error' timeout");
		set_test_name("If the response is valid but has a non-200 HTTP code, and there is no retry_in key, then it schedules a refresh in the near future according to the last 'has_errors' timeout");

		set_test_name("If the response is valid then updates the segment's server list");
		set_test_name("If the response is valid then updates the segment's timeouts");
		set_test_name("If the response is valid and indicates that the key now belongs to a different segment, and that segment already exists, then it moves the key to that segment");
		set_test_name("If the response is valid and indicates that the key now belongs to a different segment, and that segment already exists, then it updates that segment's server list");
		set_test_name("If the response is valid and indicates that the key now belongs to a different segment, and that segment does not already exist, then it creates a new segment and moves the key there");
		set_test_name("If the response is valid and indicates that the key now belongs to a different segment, and that segment does not already exist, then it creates a new segment and populates its server list");
		set_test_name("If the response is valid then it schedules a refresh in the near future according to the 'all_healthy' timeout");
	}
}
