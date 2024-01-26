from requests.exceptions import SSLError
from watson_developer_cloud import DiscoveryV1, WatsonApiException
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud import ConversationV1
import json
import datetime, time
import os


RETRYABLE_CODES = {504, 400, 503, 500}
PERPETUAL_RETRYABLE_CODES = {429}       # Codes to retry that have no limit on the number of retry attempts

class WDSConnection:
    RETRY_INTERVAL = 5
    MAX_RETRY = 5
    NO_ENRICHMENT_CONFIG = 'No enrichment'

    def __init__(self, config_name='KCCI_events', discovery_version='2017-11-11'):

        # if 'VCAP_SERVICES' in os.environ:
        #     vcap = json.loads(os.getenv('VCAP_SERVICES'))
        #     if 'discovery' in vcap:
        #         creds = vcap['discovery'][0]['credentials']
        #         user = creds['username']
        #         password = creds['password']
        # elif os.path.isfile('vcap-local.json'):
        #         with open('vcap-local.json') as f:
        #             vcap = json.load(f)
        #             creds = vcap['services']['discovery'][0]['credentials']
        #             user = creds['username']
        #             password = creds['password']
        # else:
        #     print('No VCAP_SERViCES nor vcap-local.json')

        self.discovery = DiscoveryV1(
            username='be86493a-b4e8-42d9-8ea9-445b0be032dd',
            password = 'mB3SKugaEivs',
            version=discovery_version)
        # ### Get your environment ID and configuration ID's
        # get enviroment
        #self.environment_id = '8183043a-c999-48f6-90aa-d429dc47440f'
        self.environment_id = self.writable_environment_id(self.discovery)
        print('WDS Environment_id=' + self.environment_id)

    # Get the writable environment ID for this Discovery instance
    def writable_environment_id(self, discovery):
        results = discovery.list_environments().get_result()
        for environment in results["environments"]:
            if not environment["read_only"]:
                return environment["environment_id"]

    def list_collections(self, retry_attempts=0):
        try:
            return self.discovery.list_collections(self.environment_id).get_result()
        except WatsonApiException as exception:
            if exception.code in RETRYABLE_CODES and retry_attempts < self.MAX_RETRY:
                print("[Retrying] Watson Error Caught: {error}".format(error=exception))
                time.sleep(self.RETRY_INTERVAL)
                return self.list_collections(retry_attempts=retry_attempts + 1)
            elif exception.code in PERPETUAL_RETRYABLE_CODES:
                print("[Retrying] Watson Error Caught: {error}".format(error=exception))
                time.sleep(self.RETRY_INTERVAL)
                return self.list_collections()
            else:
                #self.logger.error("[Aborting] Watson Error Caught: {error}".format(error=exception))
                print("[Aborting] Watson Error Caught: {error}".format(error=exception))
                raise
        except SSLError:
            print("[Retrying] Random SSLError occurred: {error}".format(error=exception))
            time.sleep(self.RETRY_INTERVAL)
            return self.list_collections()
        except Exception:
            if retry_attempts < self.MAX_RETRY:
                print("[Retrying] Unknown exception has occurred: {error}".format(error=exception))
                time.sleep(self.RETRY_INTERVAL)
                return self.list_collections(retry_attempts=retry_attempts + 1)
            else:
                print("[Aborting] Unknown exception has occurred: {error}".format(error=exception))
                raise

    def get_wds_collection(self, collection_name):
        collections = self.list_collections()
        for item in collections['collections']:
            if item['name'] == collection_name:
                return item['collection_id']
    def load_json_wds(self, json_to_load, doc_name, collection):
        return self.add_document(self.environment_id,
                                 collection,
                                 file=json.dumps(json_to_load),
                                 file_content_type='application/json',
                                 filename=doc_name)

    def add_document(self, *args, **kwargs):
        try:
            return self.discovery.add_document(*args, **kwargs).get_result()
        except WatsonApiException as exception:
            print(exception)
