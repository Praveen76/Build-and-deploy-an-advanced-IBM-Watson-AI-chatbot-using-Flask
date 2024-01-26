import json
import queue
import sys
import threading
import datetime, time

from watson_developer_cloud import WatsonApiException
from utils.wds_connection_advanced import WDSConnectionAdvanced

class KogneticsUploadThread:
    def __init__(self, threadcount = 5):
        self.counts = {}
        self.error_entities = {}

        self.wds = WDSConnectionAdvanced()
        self.environment_id = self.wds.environment_id

        # Client Collection
        self.collection_name = 'KCCI-KOGNETICS-DATA'
        self.collection_id = self.wds.get_wds_collection(self.collection_name)

        self.queue = queue.Queue()
        self.uploadedQueue = queue.Queue()
        self.thread_count = threadcount
        self.wait_until = 0
        for _ in range(self.thread_count):
            threading.Thread(target = self.process_queue, daemon = True).start()


    def process_queue(self):
        item = self.queue.get()

        while item:
            if item is None:
                break

            # Pause this thread when we need to back off pushing
            wait_for = self.wait_until - time.perf_counter()
            if wait_for > 0:
                print(datetime.datetime.now().strftime(
                    "%a, %d %B %Y %I:%M:%S") + ' process_queue about to sleep for ' + str(wait_for) + 'seconds')
                time.sleep(wait_for)
            content = item[0]
            file_name = item[1]

            try:
                uploadResponse = self.wds.load_json_wds(content, file_name, self.collection_id)
                #uploadResponse = self.kognetics_news_ingestion.load(content, file_name, self.collection_id)
                # Save documentID and payload so we can check status later and resubmit if necessary
                docID = uploadResponse['document_id']
                self.uploadedQueue.put((docID, content, file_name))
            except:
                exception = sys.exc_info()[1]
                if isinstance(exception, WatsonApiException):
                    if exception.code == 429 or exception.code == 504 or exception.code == 400 or exception.code == 503:
                        self.wait_until = time.perf_counter() + 5
                        self.queue.put(item)
                    else:
                        print("Failing {} due to {}"
                              .format(item, exception))
                        self.counts[str(exception.code)] = self.counts.get(
                            str(exception.code), 0) + 1
                        self.error_entities[item] = self.error_entities.get(item, 0) + 1
                else:
                    print("Failing {} due to {}"
                          .format(item, exception))
                    self.counts["UNKNOWN"] = self.counts.get("UNKNOWN", 0) + 1
                    self.error_entities[item] = self.error_entities.get(item, 0) + 1

            self.queue.task_done()
            item = self.queue.get()

    def put_in_queue(self, item):
        """Push an item into our work queue."""
        self.queue.put(item)

    def processStatusQ(self, statusQ, retryQ):
        print(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S") + ' processStatusQ entry qsize:' + str(
            statusQ.qsize()))

        while not statusQ.empty():
            item = statusQ.get()
            docID, content, file_name = item

            try:
                curResponse = self.wds.get_document_status(self.wds.environment_id, self.collection_id, docID)
                curStatus = curResponse['status']
                # print(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S") + ' processStatusQ ' + str(file_name) + ' - ' + str(docID) + ' ' + str(curStatus))
                if curStatus.startswith('available'):
                    # Process successfully, continue to next item in statusQ
                    continue
                if curStatus == 'processing':
                    # Discovery is still processing it so put it on the retryQ to check again later
                    retryQ.put((docID, content, file_name))
                if curStatus.startswith('failed'):
                    # Attempt to resend this document
                    print(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S") + ' processStatusQ ' + str(
                        file_name) + ' - ' + str(docID) + ' ' + str(curStatus))
                    print(datetime.datetime.now().strftime(
                        "%a, %d %B %Y %I:%M:%S") + ' processStatusQ re-uploading document.  Original doc status:' + json.dumps(
                        curResponse))
                    print('original json=' + json.dumps(content))
                    uploadResponse = self.wds.load_json_wds(content, file_name.strip, self.collection_id)
                    # Save documentID and payload so we can check status later and resubmit if necessary
                    newdocID = uploadResponse['document_id']
                    retryQ.put((newdocID, content, file_name))
            except:
                exception = sys.exc_info()[1]
                print(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S") + ' processStatusQ Exception ' + str(
                    file_name) + ' - ' + str(docID) + ' ' + str(exception))
                if isinstance(exception, WatsonApiException):
                    if exception.code == 429 or exception.code == 504 or exception.code == 400 or exception.code == 503:
                        # Retry this document
                        retryQ.put(item)
                    else:
                        print("Failing {} due to {}"
                              .format(item, exception))
                        self.counts[str(exception.code)] = self.counts.get(
                            str(exception.code), 0) + 1
                        self.error_entities[item] = self.error_entities.get(item, 0) + 1
                else:
                    print("Failing {} due to {}"
                          .format(item, exception))
                    self.counts["UNKNOWN"] = self.counts.get("UNKNOWN", 0) + 1
                    # self.error_entities[item] = self.error_entities.get(item, 0) +1

        print(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S") + ' processStatusQ exit retry qsize:' + str(
            retryQ.qsize()))

    def checkDocumentsStatus(self):
        retryQ = queue.Queue()
        retryQ2 = queue.Queue()
        # First go through the uploadedQueue and check status of all docs uploaded
        # If any are still being processed or were retried they will be put onto the retryQ
        self.processStatusQ(self.uploadedQueue, retryQ)
        sleepTime = 30
        retries = 0
        while retries < 20:
            if not retryQ.empty():
                print(datetime.datetime.now().strftime(
                    "%a, %d %B %Y %I:%M:%S") + ' checkDocumentsStatus() about to sleep ' + str(
                    sleepTime) + ' to wait for rechecking document status.  Remaining docs=' + str(retryQ.qsize()))
                time.sleep(sleepTime)
                self.processStatusQ(retryQ, retryQ2)
            elif not retryQ2.empty():
                print(datetime.datetime.now().strftime(
                    "%a, %d %B %Y %I:%M:%S") + ' checkDocumentsStatus() about to sleep ' + str(
                    sleepTime) + ' to wait for rechecking document status.  Remaining docs2=' + str(retryQ2.qsize()))
                time.sleep(sleepTime)
                self.processStatusQ(retryQ2, retryQ)
            else:
                # No more docs to check
                break
            retries += 1
            if retries < 3:
                sleepTime += 300

    def finish(self):
        """Block until all tasks are done then return runtime."""
        print(datetime.datetime.now().strftime("%a, %d %B %Y %I:%M:%S") + ' client_upload_thread finish() Entry')
        self.queue.join()
        for _ in range(self.thread_count):
            self.queue.put(None)
        self.checkDocumentsStatus()
        for code, count in self.counts.items():
            print("The error code", code, "was returned", count, "time(s).")
        for entity_id, count in self.error_entities.items():
            print('The entity id {0} has failed {1} times'.format(entity_id, count))