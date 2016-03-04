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
			self->keyInfoApiLookupFinished(key, code, httpCode,
				responseBody, errorBuffer);
			delete this;
		}
	};

	class KeyInfo {
		SegmentPtr segment;
		ev_tstamp lastCheckTime;
		ev_tstamp lastRejectionErrorTime;
		unsigned int recheckTimeoutWhenAllHealthy;
		unsigned int recheckTimeoutWhenHaveErrors;
		bool rejectionActive;
		bool lookingUp;

		KeyInfo()
			: lastCheckTime(0),
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
	struct ev_timer timer;
	size_t peakSize;


	KeyInfoPtr findOrCreateKeyInfo(const HashedStaticString &key) {
		KeyInfoPtr *keyInfo;

		if (keyInfos.lookup(key, &keyInfo)) {
			return *keyInfo;
		} else {
			KeyInfoPtr newKeyInfo(make_shared<KeyInfo>());
			if (initiateApiLookup(newKeyInfo, key)) {
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

		STAILQ_FOREACH(segment, &segments, segmenterFields.nextToBeForwarded) {
			segment->segmenterFields.toBeForwarded = false;
		}

		batcher.schedule(segments);

		STAILQ_FOREACH_SAFE(segment, &segments, segmenterFields.nextToBeForwarded, nextSegment) {
			STAILQ_NEXT(segment, segmenterFields.nextToBeForwarded) = NULL;
		}

		STAILQ_INIT(&segments);
	}

	void rescheduleNextKeyInfoRefresh() {
		// TODO
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
	virtual bool initiateApiLookup(const KeyInfoPtr &keyInfo, const string &key) {
		TransferInfo *transferInfo;
		CURL *curl;

		curl = curl_easy_init();
		if (curl == NULL) {
			return false;
		}

		transferInfo = new TransferInfo(this, curl, key);
		curl_easy_setopt(curl, CURLOPT_URL, apiUrl);
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
		curlLibevIntegration.initialize(loop, multi);
	}

	~Segmenter() {
		Segment *segment, *nextSegment;

		STAILQ_FOREACH_SAFE(segment, &segments, segmenterFields.next, nextSegment) {
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

				if (!segment->segmenterFields.toBeForwarded) {
					segment->segmenterFields.toBeForwarded = true;
					STAILQ_INSERT_TAIL(&segmentsToForward, segment, segmenterFields.nextToBeForwarded);
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

	void keyInfoApiLookupFinished(const string &key, CURLcode code, long httpCode,
		const string &body, const char *errorBuffer)
	{
		handleKeyInfoApiLookupSuccessResponse(key, doc);
	}

	void handleKeyInfoApiLookupSuccessResponse(const string &key, const Json::Value &doc) {
		SegmentList segments;
		Segment;

		SEGMENTS
		batcher.schedule(segments);
	}
};
