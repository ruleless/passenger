class AbstractBatcher {
public:
	~AbstractBatcher();
	virtual void schedule(SegmentList &segments) = 0;
};

class Segmenter {
private:
	class TransferInfo: public CurlLibevIntegration::TransferInfo {
	public:
		Segmenter *self;
		CURL *curl;
		string key;
		char errorBuffer[CURL_ERROR_SIZE];
		STAILQ_ENTRY(TransferInfo) next;

		TransferInfo(Segmenter *_self, CURL *_curl, const string &_key)
			: self(_self),
			  curl(_curl),
			  key(_key)
		{
			errorBuffer[0] = '\0';
			STAILQ_NEXT(this, next) = NULL;
		}

		virtual void finish(CURL *curl, CURLcode code) {
			long httpCode = -1;
			P_ASSERT_EQ(this->curl, curl);

			if (code == CURL_OK) {
				curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
			}
			STAILQ_REMOVE(&self->transferInfos, this, TransferInfo, next);
			curl_easy_cleanup(curl);
			self->apiLookupFinished(key, code, httpCode,
				responseBody, errorBuffer);
			delete this;
		}
	};

	class KeyInfo {
		SegmentPtr segment;
		string key;
		ev_tstamp lastRefreshSuccessTime;
		ev_tstamp lastRefreshErrorTime;
		ev_tstamp lastRejectionErrorTime;
		unsigned int recheckTimeoutWhenAllHealthy;
		unsigned int recheckTimeoutWhenHaveErrors;
		bool rejectionActive;
		bool lookingUp;

		KeyInfo(const string &_key)
			: key(_key),
			  lastRefreshSuccessTime(0),
			  lastRefreshErrorTime(0),
			  lastRejectionErrorTime(0),
			  recheckTimeoutWhenAllHealthy(5 * 60),
			  recheckTimeoutWhenHaveErrors(60),
			  rejectionActive(false),
			  lookingUp(false)
			{ }
	};

	STAILQ_HEAD(TransferInfoList, TransferInfo);
	typedef boost::shared_ptr<KeyInfo> KeyInfoPtr;

	struct ev_loop *loop;
	CURLM *multi;
	CurlLibevIntegration curlLibevIntegration;
	SegmentList segments;
	StringKeyTable<KeyInfoPtr> keyInfos;
	TransferInfoList transferInfos;
	AbstractBatcher *batcher;
	struct ev_timer timer;
	size_t peakSize;


	KeyInfoPtr findOrCreateKeyInfo(const HashedStaticString &key) {
		KeyInfoPtr *keyInfo;

		if (keyInfos.lookup(key, &keyInfo)) {
			return *keyInfo;
		} else {
			KeyInfoPtr newKeyInfo(make_shared<KeyInfo>(key));
			if (initiateApiLookup(newKeyInfo)) {
				keyInfos.insert(key, newKeyInfo);
				STAILQ_INSERT_TAIL(&unknownKeyInfos, newKeyInfo.get(), next);
				return newKeyInfo;
			} else {
				return KeyInfoPtr();
			}
		}
	}

	void forwardToBatcher(SegmentList &segments) {
		Segment *segment, *nextSegment;

		STAILQ_FOREACH(segment, &segments, nextScheduledForBatching) {
			segment->scheduledForBatching = false;
		}

		batcher.schedule(segments);
		assert(STAILQ_EMPTY(&segments->incomingTransactions));

		STAILQ_FOREACH_SAFE(segment, &segments, nextScheduledForBatching, nextSegment) {
			STAILQ_NEXT(segment, scheduledForBatching) = NULL;
		}

		STAILQ_INIT(&segments);
	}

	void forwardToBatcher(Segment *segment) {
		SegmentList segments;
		STAILQ_INIT(&segments);
		STAILQ_INSERT_TAIL(&segments, segment, nextScheduledForBatching);
		batcher.schedule(segments);
		assert(STAILQ_EMPTY(&segments->incomingTransactions));
	}

	void rescheduleNextKeyInfoRefresh() {
		ev_tstamp nextTimeout = std::numeric_limits<ev_tstamp>::max();
		StringKeyTable<KeyInfoPtr>::ConstIterator it(&keyInfos);

		while (*it != NULL) {
			const KeyInfoPtr &keyInfo = it.getValue();

			if (keyInfo->lookingUp) {
				it.next();
				continue;
			}

			if (keyInfo->rejectionActive) {
				nextTimeout = std::min(nextTime, keyInfo->lastRejectionErrorTime
					+ keyInfo->recheckTimeoutWhenHaveErrors);
			} else {
				nextTimeout = std::min(nextTime, keyInfo->lastCheckTime
					+ keyInfo->recheckTimeoutWhenAllHealthy);
			}

			it.next();
		}

		if (nextTimeout == this->nextTimeout) {
			return;
		}

		if (nextTime == std::numeric_limits<ev_tstamp>::max()) {
			ev_timer_stop(loop, &timer);
		} else {
			ev_timer_stop(loop, &timer);
			ev_timer_set(&timer, nextTime - ev_now(loop), 0);
			ev_timer_start(loop, &timer);
		}
		this->nextTimeout = nextTimeout;
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

	void updateSegmentInfo(Segment *segment, const Json::Value &doc) {
		segment->lastCheckTime = ev_now(loop);
		if (doc.isMember("retry_in")) {
			segment->recheckTimeoutWhenAllHealthy = getJsonUintField(
				doc["retry_in"], "all_healthy",
				segment->recheckTimeoutWhenAllHealthy);
			segment->recheckTimeoutWhenHaveErrors = getJsonUintField(
				doc["retry_in"], "has_errors",
				segment->recheckTimeoutWhenHaveErrors);
		}
	}

	void handleApiResponse(const string &keyInfo, long httpCode,
		const string &body)
	{
		Json::Reader reader;
		Json::Value doc;

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
		keyInfo->lastRefreshErrorTime = ev_now(loop);
		setApiLookupError(
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned an invalid response (unparseable). "
			+ "Parse error: " + parseErrorMessage
			+ "; key: " + keyInfo->key,
			+ "; HTTP code: " + toString(httpCode)
			+ "; body: \"" + cEscapeString(body) + "\""));
	}

	void handleApiResponseInvalid(const KeyInfoPtr &keyInfo, unsigned int httpCode,
		const string &body)
	{
		keyInfo->lastRefreshErrorTime = ev_now(loop);
		setApiLookupError(
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned a invalid response (parseable, but does not comply to expected structure)."
			+ " Key: " + keyInfo->key,
			+ "; HTTP code: " + toString(httpCode)
			+ "; body: \"" + cEscapeString(body) + "\""));
	}

	void handleApiResponseErrorMessage(const KeyInfoPtr &keyInfo,
		const Json::Value &doc)
	{
		P_ASSERT_EQ(doc["status"].asString(), "error");

		string message = "Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + docURL
			+ " returned an error. Message from server: "
			+ doc["message"].asString()
			+ "; key: " + keyInfo->key;
		if (doc.isMember("error_id")) {
			message.append("; error ID: ");
			message.append(doc["error_id"].asString());
		}
		setApiLookupError(message);

		keyInfo->lastRefreshErrorTime = ev_now(loop);
		if (doc.isMember("retry_in")) {
			keyInfo->recheckTimeoutWhenHaveErrors = doc["retry_in"].asUInt();
		}
	}

	void handleApiResponseInvalidHttpCode(const KeyInfoPtr &keyInfo,
		unsigned int httpCode, const string &body)
	{
		keyInfo->lastRefreshErrorTime = ev_now(loop);
		setApiLookupError(
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " returned a invalid response."
			+ " Key: " + keyInfo->key,
			+ "; HTTP code: " + toString(httpCode)
			+ "; body: \"" + cEscapeString(body) + "\""));
	}

	void handleApiSuccessResponse(const string &key, const Json::Value &doc) {
		KeyInfoPtr keyInfo(keyInfos.lookupCopy(key));
		assert(keyInfos != NULL);
		string segmentKey(createSegmentKey(doc));
		Segment *segment;

		keyInfo->lastRefreshSuccessTime = ev_now(loop);

		if (keyInfo->segment == NULL) {
			// Create new segment
			segment = new Segment(segmentKey);
			updateSegmentInfo(segment, doc);
			keyInfo->segment.reset(segment);
			STAILQ_INSERT_TAIL(&segments, segment, nextInSegmenterList);

			// Move all queued transactions with the current key
			// into this segment
			Transaction *transaction, *nextTransaction;
			STAILQ_FOREACH_SAFE(&queue, transaction, next, nextTransaction) {
				if (transaction->getUnionStationKey() == key) {
					STAILQ_REMOVE(&queue, transaction, Transaction, next);
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
			updateSegmentInfo(segment, doc);
			keyInfo->segment.reset(segment);

		} else {
			updateSegmentInfo(keyInfo->segment, doc);
		}
	}

	void handleApiLookupPerformError(const KeyInfoPtr &keyInfo,
		CURLcode code, const char *errorBuffer)
	{
		setApiLookupError(
			"Unable to fetch a list of Union Station gateway servers. "
			"The Union Station load balancing server " + manifestURL
			+ " appears to be down. Error message: "
			+ errorBuffer);
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
	virtual bool initiateApiLookup(const KeyInfoPtr &keyInfo) {
		TransferInfo *transferInfo;
		CURL *curl;

		curl = curl_easy_init();
		if (curl == NULL) {
			return false;
		}

		transferInfo = new TransferInfo(this, curl, keyInfo->key);
		curl_easy_setopt(curl, CURLOPT_URL, manifestURL.c_str());
		curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long) CURL_HTTP_VERSION_2);
		curl_easy_setopt(curl, CURLOPT_PIPEWAIT, (long) 1);
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, (long) 1);
		curl_easy_setopt(curl, CURLOPT_PRIVATE, transferInfo);
		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, transferInfo->errorBuf);
		curl_easy_setopt(curl, CURLOPT_USERAGENT, PROGRAM_NAME " " PASSENGER_VERSION);
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, (long) 1);
		curl_easy_setopt(curl, CURLOPT_NOPROGRESS, (long) 1);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlDataReceived);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, transferInfo);

		CURLMcode ret = curl_multi_add_handle(multi, curl);
		if (ret != CURLM_OK) {
			curl_easy_cleanup(curl);
			delete transferInfo;
			return false;
		}

		keyInfo->lookingUp = true;
		STAILQ_INSERT_TAIL(&transferInfos, transferInfo, next);
		return true;
	}

public:
	Segmenter()
		: keys(2, CACHE_LINE_SIZE - ESTIMATED_MALLOC_OVERHEAD)
	{
		multi = curl_multi_init();
		curl_multi_setopt(multi, CURLMOPT_PIPELINING, (long) CURLPIPE_MULTIPLEX);
		curlLibevIntegration.initialize(loop, multi);
	}

	~Segmenter() {
		Segment *segment, *nextSegment;

		STAILQ_FOREACH_SAFE(segment, &segments, nextInSegmenterList, nextSegment) {
			segment->unref();
		}

		STAILQ_FOREACH_SAFE(transferInfo, &transferInfos, next, nextTransferInfo) {
			curl_multi_remove_handle(multi, transferInfo->curl);
			curl_easy_cleanup(transferInfo->curl);
			delete transferInfo;
		}

		curlLibevIntegration.destroy();
		curl_multi_cleanup(multi);
	}

	void schedule(TransactionList &transactions, size_t totalBodySize, unsigned int count,
		unsigned int &bytesAdded, unsigned int &nAdded)
	{
		SegmentList segmentsToForward;
		Segment *segment;
		bool shouldRescheduleNextKeyInfoRefresh = false;

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
				shouldRescheduleNextKeyInfoRefresh = true;
				STAILQ_INSERT_TAIL(&queue, transaction, next);
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

		if (shouldRescheduleNextKeyInfoRefresh) {
			rescheduleNextKeyInfoRefresh();
		}
	}

	void apiLookupFinished(const string &key, CURLcode code, long httpCode,
		const string &body, const char *errorBuffer)
	{
		KeyInfoPtr keyInfo(keys.lookupCopy(key));
		assert(keyInfo != NULL);

		keyInfo->lookingUp = false;

		if (code == CURLE_OK) {
			handleApiResponse(keyInfo, httpCode, body);
		} else {
			handleApiLookupPerformError(keyInfo, code, errorBuffer);
		}
	}
};
