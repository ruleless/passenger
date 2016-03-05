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
#ifndef _PASSENGER_UST_ROUTER_REMOTE_SINK_SEGMENTER_H_
#define _PASSENGER_UST_ROUTER_REMOTE_SINK_SEGMENTER_H_

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <string>
#include <algorithm>
#include <limits>
#include <cstddef>
#include <cassert>
#include <cstring>

#include <curl/curl.h>
#include <ev.h>
#include <psg_sysqueue.h>

#include <Logging.h>
#include <CurlLibevIntegration.h>
#include <Algorithms/MovingAverage.h>
#include <DataStructures/StringKeyTable.h>
#include <Utils/JsonUtils.h>
#include <Utils/StrIntUtils.h>
#include <UstRouter/RemoteSink/Segment.h>

namespace Passenger {
namespace UstRouter {
namespace RemoteSink {

using namespace std;


class AbstractBatcher {
public:
	~AbstractBatcher() { }
	virtual void schedule(SegmentList &segments) = 0;
};

class Segmenter {
protected:
	struct KeyInfo {
		SegmentPtr segment;
		const string key;
		ev_tstamp lastLookupSuccessTime;
		ev_tstamp lastLookupErrorTime;
		ev_tstamp lastRejectionErrorTime;
		unsigned int refreshTimeoutWhenAllHealthy;
		unsigned int refreshTimeoutWhenHaveErrors;
		string lastErrorMessage;
		bool rejectionActive;
		bool lookingUp;

		KeyInfo(const string &_key)
			: key(_key),
			  lastLookupSuccessTime(0),
			  lastLookupErrorTime(0),
			  lastRejectionErrorTime(0),
			  refreshTimeoutWhenAllHealthy(5 * 60),
			  refreshTimeoutWhenHaveErrors(60),
			  rejectionActive(false),
			  lookingUp(false)
			{ }
	};

	typedef boost::shared_ptr<KeyInfo> KeyInfoPtr;

private:
	static const unsigned int ESTIMATED_CACHE_LINE_SIZE = 64;
	static const unsigned int ESTIMATED_MALLOC_OVERHEAD = 16;

	class TransferInfo: public CurlLibevIntegration::TransferInfo {
	public:
		Segmenter * const self;
		CURL * const curl;
		const string key;
		const ev_tstamp startTime;
		string responseBody;
		char errorBuffer[CURL_ERROR_SIZE];
		STAILQ_ENTRY(TransferInfo) next;

		TransferInfo(Segmenter *_self, CURL *_curl, const string &_key, ev_tstamp _now)
			: self(_self),
			  curl(_curl),
			  key(_key),
			  startTime(_now)
		{
			errorBuffer[0] = '\0';
			STAILQ_NEXT(this, next) = NULL;
		}

		virtual void finish(CURL *curl, CURLcode code) {
			long httpCode = -1;
			P_ASSERT_EQ(this->curl, curl);

			if (code == CURLE_OK) {
				curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
			}
			STAILQ_REMOVE(&self->transferInfos, this, TransferInfo, next);
			curl_easy_cleanup(curl);
			self->apiLookupFinished(key, startTime, code, httpCode,
				responseBody, errorBuffer);
			delete this;
		}
	};

	STAILQ_HEAD(TransferInfoList, TransferInfo);

	struct ev_loop * const loop;
	AbstractBatcher * const batcher;
	CURLM *multi;
	CurlLibevIntegration curlLibevIntegration;
	string manifestURL;
	struct ev_timer timer;
	ev_tstamp nextKeyInfoRefreshTime;
	ev_tstamp lastErrorTime;
	string lastErrorMessage;

	SegmentList segments;
	StringKeyTable<KeyInfoPtr> keyInfos;
	TransferInfoList transferInfos;
	TransactionList queued;

	size_t limit;
	size_t bytesQueued;
	size_t peakSize;
	size_t bytesForwarded;
	size_t bytesDropped;
	unsigned int nQueued;
	unsigned int nForwarded;
	unsigned int nDropped;
	double avgKeyInfoLookupTime;


	KeyInfoPtr findOrCreateKeyInfo(const HashedStaticString &key) {
		KeyInfoPtr *keyInfo;

		if (keyInfos.lookup(key, &keyInfo)) {
			return *keyInfo;
		} else {
			KeyInfoPtr newKeyInfo(boost::make_shared<KeyInfo>(key));
			if (initiateApiLookup(newKeyInfo)) {
				keyInfos.insert(key, newKeyInfo);
				return newKeyInfo;
			} else {
				return KeyInfoPtr();
			}
		}
	}

	void forwardToBatcher(SegmentList &segments) {
		Segment *segment;

		STAILQ_FOREACH(segment, &segments, nextScheduledForBatching) {
			segment->scheduledForBatching = false;
		}

		batcher->schedule(segments);
		assert(STAILQ_EMPTY(&segments));
	}

	void forwardToBatcher(Segment *segment) {
		SegmentList segments;
		STAILQ_INIT(&segments);
		STAILQ_INSERT_TAIL(&segments, segment, nextScheduledForBatching);
		batcher->schedule(segments);
		assert(STAILQ_EMPTY(&segments));
	}

	void rescheduleNextKeyInfoRefresh() {
		ev_tstamp nextTimeout = std::numeric_limits<ev_tstamp>::max();
		StringKeyTable<KeyInfoPtr>::ConstIterator it(keyInfos);

		while (*it != NULL) {
			const KeyInfoPtr &keyInfo = it.getValue();

			if (keyInfo->lookingUp) {
				it.next();
				continue;
			}

			if (keyInfo->rejectionActive) {
				nextTimeout = std::min(nextTimeout, keyInfo->lastRejectionErrorTime
					+ keyInfo->refreshTimeoutWhenHaveErrors);
			} else {
				nextTimeout = std::min(nextTimeout, keyInfo->lastLookupSuccessTime
					+ keyInfo->refreshTimeoutWhenAllHealthy);
			}

			it.next();
		}

		if (nextTimeout != std::numeric_limits<ev_tstamp>::max()) {
			// Align the time to a multiple of 5 seconds to save power on laptops.
			nextTimeout = timeToNextMultipleD(5, nextTimeout);
		}

		if (nextTimeout == this->nextKeyInfoRefreshTime) {
			// Scheduled time not changed. No action required.
			return;
		}

		this->nextKeyInfoRefreshTime = nextTimeout;
		if (ev_is_active(&timer)) {
			ev_timer_stop(loop, &timer);
		}
		if (nextTimeout != std::numeric_limits<ev_tstamp>::max()) {
			P_DEBUG("[RemoteSink segmenter] Rescheduling next key info refresh time: "
				<< distanceOfTimeInWords(ev_now(loop), nextTimeout) << " from now");
			ev_timer_set(&timer, nextTimeout - ev_now(loop), 0);
			ev_timer_start(loop, &timer);
		}
	}

	string createSegmentKey(const Json::Value &doc) {
		return stringifyJson(doc["targets"]);
	}

	Segment *findSegment(const StaticString &segmentKey) {
		Segment *segment;

		STAILQ_FOREACH(segment, &segments, nextInSegmenterList) {
			if (segmentKey == segment->segmentKey) {
				return segment;
			}
		}

		return NULL;
	}

	void updateKeyInfoFromManifest(const KeyInfoPtr &keyInfo, const Json::Value &doc) {
		if (doc.isMember("retry_in")) {
			keyInfo->refreshTimeoutWhenAllHealthy = getJsonUintField(
				doc["retry_in"], "all_healthy",
				keyInfo->refreshTimeoutWhenAllHealthy);
			keyInfo->refreshTimeoutWhenHaveErrors = getJsonUintField(
				doc["retry_in"], "has_errors",
				keyInfo->refreshTimeoutWhenHaveErrors);
		}
	}

	void updateSegmentFromManifest(const SegmentPtr &segment, const Json::Value &doc) {
		// TODO
	}

	bool validateApiResponse(const Json::Value &doc) const {
		if (OXT_UNLIKELY(!doc.isObject())) {
			return false;
		}
		if (OXT_UNLIKELY(doc.isMember("status"))) {
			return false;
		}
		if (OXT_LIKELY(doc["status"].asString() == "ok")) {
			if (OXT_UNLIKELY(!doc.isMember("targets") || !doc["targets"].isArray())) {
				return false;
			}

			Json::Value::const_iterator it, end = doc["targets"].begin();
			for (it = doc["targets"].begin(); it != end; it++) {
				const Json::Value &target = *it;
				if (OXT_UNLIKELY(!target.isObject())) {
					return false;
				}
				if (OXT_UNLIKELY(!target.isMember("base_url") || !target["base_url"].isString())) {
					return false;
				}
				if (OXT_UNLIKELY(!target.isMember("weight") || !target["weight"].isUInt())) {
					return false;
				}
				if (target["weight"].asUInt() == 0) {
					return false;
				}
			}
		} else if (doc["status"].asString() == "error") {
			if (!doc.isMember("message") || !doc["message"].isString()) {
				return false;
			}
			if (doc.isMember("retry_in") && !doc["retry_in"].isUInt()) {
				return false;
			}
			if (doc.isMember("error_id") && !doc["error_id"].isString()) {
				return false;
			}
		} else {
			return false;
		}

		return true;
	}

	void handleApiResponse(const KeyInfoPtr &keyInfo, long httpCode,
		const string &body)
	{
		Json::Reader reader;
		Json::Value doc;

		// TODO: drop queued transactions
		// TODO: schedule next lookup
		// TODO: create server list

		if (OXT_UNLIKELY(!reader.parse(body, doc, false))) {
			handleApiResponseParseError(keyInfo, httpCode, body,
				reader.getFormattedErrorMessages());
			return;
		}
		if (OXT_UNLIKELY(!validateApiResponse(doc))) {
			handleApiResponseInvalid(keyInfo, httpCode, body);
			return;
		}
		if (OXT_UNLIKELY(doc["status"].asString() != "ok")) {
			handleApiResponseErrorMessage(keyInfo, doc);
			return;
		}
		if (OXT_UNLIKELY(httpCode != 200)) {
			handleApiResponseInvalidHttpCode(keyInfo, httpCode, body);
			return;
		}

		handleApiSuccessResponse(keyInfo, doc);
	}

	void handleApiResponseParseError(const KeyInfoPtr &keyInfo, unsigned int httpCode,
		const string &body, const string &parseErrorMessage)
	{
		setApiLookupError(keyInfo,
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned an invalid response (unparseable). "
			+ "Parse error: " + parseErrorMessage
			+ "; key: " + keyInfo->key
			+ "; HTTP code: " + toString(httpCode)
			+ "; body: \"" + cEscapeString(body) + "\"");
	}

	void handleApiResponseInvalid(const KeyInfoPtr &keyInfo, unsigned int httpCode,
		const string &body)
	{
		setApiLookupError(keyInfo,
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned a invalid response (parseable, but does not comply to expected structure)."
			+ " Key: " + keyInfo->key
			+ "; HTTP code: " + toString(httpCode)
			+ "; body: \"" + cEscapeString(body) + "\"");
	}

	void handleApiResponseErrorMessage(const KeyInfoPtr &keyInfo,
		const Json::Value &doc)
	{
		P_ASSERT_EQ(doc["status"].asString(), "error");

		string message = "Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned an error. Message from server: "
			+ doc["message"].asString()
			+ "; key: " + keyInfo->key;
		if (doc.isMember("error_id")) {
			message.append("; error ID: ");
			message.append(doc["error_id"].asString());
		}
		setApiLookupError(keyInfo, message);

		if (doc.isMember("retry_in")) {
			keyInfo->refreshTimeoutWhenHaveErrors = doc["retry_in"].asUInt();
		}
	}

	void handleApiResponseInvalidHttpCode(const KeyInfoPtr &keyInfo,
		unsigned int httpCode, const string &body)
	{
		setApiLookupError(keyInfo,
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned a invalid response."
			+ " Key: " + keyInfo->key
			+ "; HTTP code: " + toString(httpCode)
			+ "; body: \"" + cEscapeString(body) + "\"");
	}

	void handleApiSuccessResponse(const KeyInfoPtr &keyInfo, const Json::Value &doc) {
		string segmentKey(createSegmentKey(doc));
		Segment *segment;

		keyInfo->lastLookupSuccessTime = ev_now(loop);

		if (keyInfo->segment == NULL) {
			// Create new segment
			segment = new Segment(segmentKey);
			updateKeyInfoFromManifest(keyInfo, doc);
			updateSegmentFromManifest(segment, doc);
			keyInfo->segment.reset(segment);
			STAILQ_INSERT_TAIL(&segments, segment, nextInSegmenterList);

			// Move all queued transactions with the current key
			// into this segment
			Transaction *transaction, *nextTransaction;
			STAILQ_FOREACH_SAFE(transaction, &queued, next, nextTransaction) {
				if (transaction->getUnionStationKey() == keyInfo->key) {
					STAILQ_REMOVE(&queued, transaction, Transaction, next);
					bytesQueued -= transaction->getBody().size();
					nQueued--;

					STAILQ_INSERT_TAIL(&segment->incomingTransactions,
						transaction, next);
					segment->bytesIncomingTransactions +=
						transaction->getBody().size();
					segment->nIncomingTransactions++;
				}
			}

			forwardToBatcher(segment);

		} else if (segmentKey != keyInfo->segment->segmentKey) {
			// Move key to another segment
			segment = findSegment(segmentKey);
			if (segment == NULL) {
				segment = new Segment(segmentKey);
				STAILQ_INSERT_TAIL(&segments, segment, nextInSegmenterList);
			}
			updateKeyInfoFromManifest(keyInfo, doc);
			updateSegmentFromManifest(segment, doc);
			keyInfo->segment.reset(segment);

		} else {
			updateKeyInfoFromManifest(keyInfo, doc);
			updateSegmentFromManifest(keyInfo->segment, doc);
		}
	}

	void handleApiLookupPerformError(const KeyInfoPtr &keyInfo,
		CURLcode code, const char *errorBuffer)
	{
		setApiLookupError(keyInfo,
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " appears to be down. Error message: "
			+ errorBuffer);
	}

	void setApiLookupError(const KeyInfoPtr &keyInfo, const string &message) {
		P_ERROR("[RemoteSink segmenter] " << message);
		lastErrorMessage = keyInfo->lastErrorMessage = message;
		lastErrorTime = keyInfo->lastLookupErrorTime = ev_now(loop);
	}

	static size_t curlDataReceived(char *ptr, size_t size, size_t nmemb, void *userdata) {
		TransferInfo *transferInfo = static_cast<TransferInfo *>(userdata);
		transferInfo->responseBody.append(ptr, size * nmemb);
		return size * nmemb;
	}

	string getRecommendedBufferLimit() const {
		return toString(peakSize * 2 / 1024) + " KB";
	}

protected:
	// Virtual so that it can be stubbed out by unit tests.
	virtual bool initiateApiLookup(const KeyInfoPtr &keyInfo) {
		TransferInfo *transferInfo;
		CURL *curl;

		P_DEBUG("[RemoteSink segmenter] Performing API lookup for key: " << keyInfo->key);

		curl = curl_easy_init();
		if (curl == NULL) {
			P_ERROR("[RemoteSink segmenter] Error creating CURL handle. Maybe we're out of memory");
			return false;
		}

		transferInfo = new TransferInfo(this, curl, keyInfo->key, ev_now(loop));
		curl_easy_setopt(curl, CURLOPT_URL, manifestURL.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long) CURL_HTTP_VERSION_2);
		curl_easy_setopt(curl, CURLOPT_PIPEWAIT, (long) 1);
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, (long) 1);
		curl_easy_setopt(curl, CURLOPT_PRIVATE, transferInfo);
		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, transferInfo->errorBuffer);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, PROGRAM_NAME " " PASSENGER_VERSION);
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, (long) 1);
		curl_easy_setopt(curl, CURLOPT_NOPROGRESS, (long) 1);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlDataReceived);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, transferInfo);

		CURLMcode ret = curl_multi_add_handle(multi, curl);
		if (ret != CURLM_OK) {
			P_ERROR("[RemoteSink segmenter] Error scheduling API lookup request: " <<
				curl_multi_strerror(ret) << " (code=" << ret << ")");
			curl_easy_cleanup(curl);
			delete transferInfo;
			return false;
		}

		keyInfo->lookingUp = true;
		STAILQ_INSERT_TAIL(&transferInfos, transferInfo, next);
		return true;
	}

	// Protected so that it can be accessed from unit tests.
	static void onTimeout(EV_P_ ev_timer *timer, int revents) {
		Segmenter *self = static_cast<Segmenter *>(timer->data);

		P_DEBUG("[RemoteSink segmenter] Time to refresh all key infos");

		StringKeyTable<KeyInfoPtr>::Iterator it(self->keyInfos);
		while (*it != NULL) {
			const KeyInfoPtr &keyInfo = it.getValue();
			if (!keyInfo->lookingUp) {
				self->initiateApiLookup(keyInfo);
			}
			it.next();
		}

		self->nextKeyInfoRefreshTime = std::numeric_limits<ev_tstamp>::max();
		self->rescheduleNextKeyInfoRefresh();
	}

public:
	Segmenter(struct ev_loop *_loop, AbstractBatcher *_batcher, const VariantMap &options)
		: loop(_loop),
		  batcher(_batcher),
		  nextKeyInfoRefreshTime(std::numeric_limits<ev_tstamp>::max()),
		  lastErrorTime(0),
		  keyInfos(2, ESTIMATED_CACHE_LINE_SIZE - ESTIMATED_MALLOC_OVERHEAD),
		  limit(options.getULL("union_station_segmenter_buffer_limit")),
		  bytesQueued(0),
		  peakSize(0),
		  bytesForwarded(0),
		  bytesDropped(0),
		  nQueued(0),
		  nForwarded(0),
		  nDropped(0),
		  avgKeyInfoLookupTime(-1)
	{
		multi = curl_multi_init();
		curl_multi_setopt(multi, CURLMOPT_PIPELINING, (long) CURLPIPE_MULTIPLEX);
		curlLibevIntegration.initialize(loop, multi);
		STAILQ_INIT(&segments);
		STAILQ_INIT(&transferInfos);
		STAILQ_INIT(&queued);

		memset(&timer, 0, sizeof(timer));
		ev_timer_init(&timer, onTimeout, 0, 0);
		timer.data = this;
	}

	virtual ~Segmenter() {
		Segment *segment, *nextSegment;
		TransferInfo *transferInfo, *nextTransferInfo;
		Transaction *transaction, *nextTransaction;

		STAILQ_FOREACH_SAFE(segment, &segments, nextInSegmenterList, nextSegment) {
			segment->unref();
		}

		STAILQ_FOREACH_SAFE(transferInfo, &transferInfos, next, nextTransferInfo) {
			curl_multi_remove_handle(multi, transferInfo->curl);
			curl_easy_cleanup(transferInfo->curl);
			delete transferInfo;
		}

		STAILQ_FOREACH_SAFE(transaction, &queued, next, nextTransaction) {
			delete transaction;
		}

		if (ev_is_active(&timer)) {
			ev_timer_stop(loop, &timer);
		}

		curlLibevIntegration.destroy();
		curl_multi_cleanup(multi);
	}

	void schedule(TransactionList &transactions, size_t totalBodySize, unsigned int count,
		size_t &bytesAdded, unsigned int &nAdded)
	{
		SegmentList segmentsToForward;
		Segment *segment;

		STAILQ_INIT(&segmentsToForward);

		bytesAdded = 0;
		nAdded = 0;
		peakSize = std::max<size_t>(peakSize, bytesQueued + totalBodySize);

		while (nAdded < count && bytesQueued <= limit) {
			Transaction *transaction = STAILQ_FIRST(&transactions);
			STAILQ_REMOVE_HEAD(&transactions, next);

			KeyInfoPtr keyInfo(findOrCreateKeyInfo(transaction->getUnionStationKey()));
			segment = keyInfo->segment.get();
			if (segment != NULL) {
				segment->bytesIncomingTransactions += transaction->getBody().size();
				segment->nIncomingTransactions++;
				STAILQ_INSERT_TAIL(&segment->incomingTransactions, transaction, next);

				if (!segment->scheduledForBatching) {
					segment->scheduledForBatching = true;
					STAILQ_INSERT_TAIL(&segmentsToForward, segment, nextScheduledForBatching);
				}

				bytesForwarded += transaction->getBody().size();
				nForwarded++;
			} else {
				assert(keyInfo->lookingUp);
				bytesQueued += transaction->getBody().size();
				nQueued++;
				STAILQ_INSERT_TAIL(&queued, transaction, next);
			}

			bytesAdded += transaction->getBody().size();
			nAdded++;
		}

		if (nAdded != count) {
			assert(bytesQueued > limit);
			assert(totalBodySize > bytesAdded);
			bytesDropped += totalBodySize - bytesAdded;
			nDropped += count - nAdded;
			P_WARN("Unable to lookup Union Station key information quickly enough. "
				"Please increase the Union Station segmenter buffer limit "
				"(recommended limit: " + getRecommendedBufferLimit() + ")");
		}

		forwardToBatcher(segments);
	}

	void apiLookupFinished(const string &key, ev_tstamp startTime, CURLcode code,
		long httpCode, const string &body, const char *errorBuffer)
	{
		KeyInfoPtr keyInfo(keyInfos.lookupCopy(key));
		assert(keyInfo != NULL);

		keyInfo->lookingUp = false;
		avgKeyInfoLookupTime = expMovingAverage(avgKeyInfoLookupTime,
			ev_now(loop) - startTime, 0.5);

		if (code == CURLE_OK) {
			handleApiResponse(keyInfo, httpCode, body);
		} else {
			handleApiLookupPerformError(keyInfo, code, errorBuffer);
		}
	}

	Json::Value inspectStateAsJson() const {
		Json::Value doc;
		unsigned long long now = ev_now(loop) * 1000000;

		doc["total_in_memory"]["size"] = byteSizeToJson(bytesQueued);
		doc["total_in_memory"]["count"] = nQueued;
		doc["total_in_memory"]["peak_size"] = byteSizeToJson(peakSize);
		doc["total_in_memory"]["limit"] = byteSizeToJson(limit);

		doc["queued"] = byteSizeAndCountToJson(bytesQueued, nQueued);
		doc["peak_size"] = byteSizeToJson(peakSize);
		doc["forwarded"] = byteSizeAndCountToJson(bytesForwarded, nForwarded);
		doc["dropped"] = byteSizeAndCountToJson(bytesDropped, nDropped);
		if (nextKeyInfoRefreshTime == std::numeric_limits<ev_tstamp>::max()) {
			doc["next_key_refresh_time"] = Json::Value(Json::nullValue);
		} else {
			doc["next_key_refresh_time"] = timeToJson(nextKeyInfoRefreshTime * 1000000, now);
		}
		if (avgKeyInfoLookupTime == -1) {
			doc["average_key_info_lookup_time"] = Json::Value(Json::nullValue);
		} else {
			doc["average_key_info_lookup_time"] = durationToJson(avgKeyInfoLookupTime * 10000000);
		}
		if (lastErrorTime == 0) {
			doc["last_error_time"] = Json::Value(Json::nullValue);
			doc["last_error_message"] = Json::Value(Json::nullValue);
		} else {
			doc["last_error_time"] = timeToJson(lastErrorTime * 1000000);
			doc["last_error_message"] = lastErrorMessage;
		}

		doc["segments"] = inspectSegmentsAsJson();
		doc["keys"] = inspectKeysAsJson();
		doc["transfers"] = inspectTransfersAsJson();

		return doc;
	}

	Json::Value inspectSegmentsAsJson() const {
		Json::Value doc;
		return doc;
	}

	Json::Value inspectKeysAsJson() const {
		Json::Value doc;
		return doc;
	}

	Json::Value inspectTransfersAsJson() const {
		Json::Value doc;
		return doc;
	}
};


} // namespace RemoteSink
} // namespace UstRouter
} // namespace Passenger

#endif /* _PASSENGER_UST_ROUTER_REMOTE_SINK_SEGMENTER_H_ */
