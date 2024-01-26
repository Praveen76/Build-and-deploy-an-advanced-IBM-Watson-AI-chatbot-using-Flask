import json
import logging
import pandas as pd
from datetime import datetime

from utils.wds_connection_advanced import WDSConnectionAdvanced
from eikon.eikon_service import EikonService
from trkd.get_eikon_data import GetEikonData
from trkd.multithread.te_upload_thread import TEUploadThread

APPLICATION_SCOPE = "Eikon.News.Ingestion"
NEWS_TIME_RANGE = 92

class NewsIngestionTE:
    def __init__(self):
        self.logger = logging.getLogger(APPLICATION_SCOPE)
        self.wds = WDSConnectionAdvanced()
        self.environment_id = self.wds.environment_id

        # Client Collection
        self.te_collection_name = 'KCCI-EIKON-DATA'
        self.client_datatype = 'KCCI-CLIENT-DEV'
        self.clients_collection_id = self.wds.get_wds_collection('KCCI-CLIENT-DEV')

        # Eikon Data Collection
        self.eikon_datatype = 'KCCI-EIKON-DATA'
        self.te_collection = self.wds.discovery.update_collection(environment_id = self.environment_id,
                                       collection_id = self.wds.get_wds_collection(self.te_collection_name),
                                       configuration_id = '',
                                       name = self.te_collection_name,
                                       description = '',
                                       language = 'en').get_result()
        try:
            self.te_collection_id = self.te_collection['collection_id']
        except:
            raise ("Collection {} does not exist".format(self.te_collection_name))


        self.rics_resolution = EikonService()
        self.api = GetEikonData()

        self.te_thread = TEUploadThread()

    def get_client_ids(self, environment_id, clients_collection_id):
        result_json = self.wds.discovery.query(environment_id, clients_collection_id, count=10000).get_result()
        df = pd.read_json(json.dumps(result_json['results']))
        df = df.drop_duplicates(subset="EntityID")
        return list(df['EntityID'])

    def ingest_to_WDS(self, sub_dict, doc_seq=0):
        sub_dict['datatype'] = self.eikon_datatype
        sub_dict['ingestionDate'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SET")
        jdata = json.dumps(sub_dict)

        try:
            self.wds.load_json_wds(jdata, "te_data{}.json".format(doc_seq), self.te_collection_id)
        except Exception as e:
            self.logger.error("[TE News Ingestion] Error in document number: " + str(doc_seq))
            self.logger.error(e)

        self.logger.info("[TE News Ingestion] Ingest " + str(doc_seq) + " documents successfully. ")

    def load(self):
        self.logger.info("[TR News Ingestion] Beginning Ingestion")
        client_ids = self.get_client_ids(self.environment_id, self.clients_collection_id)
        list_len = len(client_ids)

        # TODO Remove this list. This is only a test
        # Client IDs whose RICs codes are present in KCCI-EIKON
        # Comcast, AT&T and CIENA

        client_ids = ['1000527949', '1000526584', '1000516254']
        for idx, entity_id in enumerate(client_ids):
            rics = self.rics_resolution.get_ric(entity_id)

            if rics:
                self.logger.info("[TE News Ingestion] Processing client {} (of {})...".format(idx, list_len))
                jdata = self.bundle_eikon_topics(entity_id, NEWS_TIME_RANGE)

                jdata['datatype'] = self.eikon_datatype
                jdata['ingestionDate'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SET")
                payload = json.dumps(jdata)

                file_name = "te_data{}.json".format(idx)

                self.te_thread.put_in_queue([payload, file_name])
                #self.ingest_to_WDS(payload, idx)

                self.logger.info("[TE News Ingestion] Client {} Ingestion Complete!".format(entity_id))

        self.logger.info("[Eikon News Ingestion] Complete!")
        print("[Eikon News Ingestion] Complete!")

    def bundle_eikon_topics(self, entity_id, time_range):
        if entity_id:
            rics = self.rics_resolution.get_ric(entity_id)

            # @TODO place default timespan value in a config file
            days_range = 92
            if time_range:
                days_range = time_range if time_range > 0 else 92


            data1 = self.api.get_recent_business_events_pao(rics, days_range)
            data2 = self.api.get_key_initiatives_overview_pao(rics, days_range)
            data3 = self.api.get_bankruptcy_related_developments(rics, days_range)
            data4 = self.api.get_business_expansion_changes(rics, days_range)
            data5 = self.api.get_litigation_developments(rics, days_range)
            data6 = self.api.get_mergers_and_acquisition_developments(rics, days_range)

            data = {'Recent_Business_Events': data1, 'Key_Initiatives_Overview': data2, 'Bankrupcy': data3,
                    'Expansion': data4, 'Litigation': data5, 'Merger_Acquisition': data6}
            return data
        else:
            return {}

