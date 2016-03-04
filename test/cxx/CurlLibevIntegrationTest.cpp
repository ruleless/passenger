#include "TestSupport.h"
#include <string>
#include <SafeLibev.h>
#include <CurlLibevIntegration.h>
#include <BackgroundEventLoop.h>

using namespace Passenger;
using namespace std;

namespace tut {
	struct CurlLibevIntegrationTest {
		class TransferInfo: public CurlLibevIntegration::TransferInfo {
		public:
			CurlLibevIntegrationTest *self;

			virtual void finish(CURL *curl, CURLcode code) {
				self->finished++;
				curl_easy_cleanup(curl);
			}
		};

		BackgroundEventLoop bg;
		TransferInfo transferInfo;
		CURLM *multi;
		CurlLibevIntegration *integration;
		string responseData;
		unsigned int finished;

		CurlLibevIntegrationTest() {
			transferInfo.self = this;
			multi = curl_multi_init();
			integration = new CurlLibevIntegration(bg.safe->getLoop(), multi);
			finished = 0;
		}

		~CurlLibevIntegrationTest() {
			bg.stop();
			delete integration;
			curl_multi_cleanup(multi);
		}

		void startLoop() {
			if (!bg.isStarted()) {
				bg.start();
			}
		}

		static size_t writeCallback(void *ptr, size_t size, size_t nmemb, void *data) {
			CurlLibevIntegrationTest *self = static_cast<CurlLibevIntegrationTest *>(data);
			self->responseData.append((const char *) ptr, size * nmemb);
			return size * nmemb;
		}

		unsigned int getFinished() {
			unsigned int result;
			bg.safe->runSync(boost::bind(&CurlLibevIntegrationTest::_getFinished, this, &result));
			return result;
		}

		void _getFinished(unsigned int *result) {
			*result = finished;
		}

		string getResponseData() {
			string result;
			bg.safe->runSync(boost::bind(&CurlLibevIntegrationTest::_getResponseData, this, &result));
			return result;
		}

		void _getResponseData(string *result) {
			*result = responseData;
		}
	};

	DEFINE_TEST_GROUP(CurlLibevIntegrationTest);

	TEST_METHOD(1) {
		CURL *curl = curl_easy_init();
		curl_easy_setopt(curl, CURLOPT_URL, "http://slashdot.org/");
		curl_easy_setopt(curl, CURLOPT_VERBOSE, (long) 0);
		curl_easy_setopt(curl, CURLOPT_PRIVATE, &transferInfo);
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, (long) 1);
		curl_easy_setopt(curl, CURLOPT_NOPROGRESS, (long) 1);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, this);
		curl_multi_add_handle(multi, curl);

		startLoop();
		EVENTUALLY(5,
			result = getFinished() == 1;
		);
		ensure(containsSubstring(getResponseData(), "Slashdot"));
	}
}
