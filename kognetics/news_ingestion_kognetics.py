import json
import logging
import pandas as pd
from datetime import datetime

from kognetics.get_kognetics_data import GetKogneticsData
from kognetics.kognetics_service import KogneticsService
from utils.wds_connection_advanced import WDSConnectionAdvanced
from kognetics.multithread.kognetics_upload_thread import KogneticsUploadThread
from intent_retrieval_watson import WAIntentClassification

APPLICATION_SCOPE = "Kognetics.News.Ingestion"

class NewsIngestionKognetics:

    def __init__(self):
        self.logger = logging.getLogger(APPLICATION_SCOPE)
        self.wds = WDSConnectionAdvanced()
        self.environment_id = self.wds.environment_id
        
        ## Client Collection
        self.client_datatype = 'KCCI-CLIENT-DEV'
        self.clients_collection_id = self.wds.get_wds_collection('KCCI-CLIENT-DEV')
        
        ## Kognetics Data Collection
        self.kognetics_datatype = 'KCCI-KOGNETICS-DATA'
        self.kog_collection_name = 'KCCI-KOGNETICS-DATA'
        self.kog_collection = self.wds.discovery.update_collection(environment_id = self.environment_id,
                                       collection_id = self.wds.get_wds_collection(self.kog_collection_name),
                                       configuration_id = '',
                                       name = self.kog_collection_name,
                                       description = '',
                                       language = 'en').get_result()
        try:
            self.kog_collection_id = self.kog_collection['collection_id']
        except TypeError:
            raise("Collection {} failed to create".format(self.kog_collection['collection_id'])) 
            
        self.permalink_obj = KogneticsService()  ## To get permalink if data available from Kognetics
        self.api_k = GetKogneticsData()
        
        self.kognetics_thread = KogneticsUploadThread()
        self.wa_api = WAIntentClassification()  ## To get intents from Watson Assistant API

    
    def get_client_ids(self):
        result_json = self.wds.discovery.query(self.wds.environment_id,
                                               self.clients_collection_id,
                                               count=10000).get_result()
        df = pd.read_json(json.dumps(result_json['results']))
        df = df.drop_duplicates(subset = "EntityID")
        return list(df['EntityID'])
    
    # def ingest_to_WDS(self, data_dict, doc_seq = 0):
    #     data_dict['datatype'] = self.kognetics_datatype
    #     data_dict['ingestionDate'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SET")
    #     jdata = json.dumps(data_dict)

    #     try:
    #         self.wds.load_json_wds(jdata, "kognetics_data{}.json".format(doc_seq), self.kog_collection_id)
    #     except Exception as e:
    #         self.logger.error("[Kognetics News Ingestion] Error in document number: " + str(doc_seq))
    #         self.logger.error(e)

    #     self.logger.info("[Kognetics News Ingestion] Ingest " + str(doc_seq) + " documents successfully. ")

    def kognetics_topics_bundle(self, entity_id, num_of_days):
        if entity_id:
            permalink = self.permalink_obj.get_permalink(entity_id)
        
            if not(permalink == 'no permalink retrieved'):
                data1 = self.api_k.get_mna_news(permalink, num_of_days)
                data1 = self.get_news_intent(data1)
                data2 = self.api_k.get_financial_news(permalink, num_of_days)
                data2 = self.get_news_intent(data2)
                data3 = self.api_k.get_tech_prod_news(permalink, num_of_days)
                data3 = self.get_news_intent(data3)
                data4 = self.api_k.get_leadership_chng_news(permalink, num_of_days)
                data4 = self.get_news_intent(data4)
                data5 = self.api_k.get_positive_signals(permalink, num_of_days)
                data5 = self.get_news_intent(data5)
                data6 = self.api_k.get_negative_signals(permalink, num_of_days)
                data6 = self.get_news_intent(data6)
                data7 = self.api_k.get_company_news_event(permalink, num_of_days)
                data7 = self.get_news_intent(data7)
                
                data_kognetics = {'Merger_Acquistion': data1, 'Financial_News': data2,
                                  'Tech_Products': data3, 'Leadership_News': data4,
                                  'Positive_Signals': data5, 'Negative_Signals': data6,
                                  'Company_News': data7}
                return data_kognetics
            else:
                data_kognetics = {'Output': 'Company Not Found In Kognetics DB'}
                return data_kognetics
        else:
            return {}
         
    def get_news_intent(self, data):
        for news in data:
            try:
                wa_response = self.wa_api.retrieve_intents(news['NewsSnippet'])
                intent = wa_response[0]
                confidence = wa_response[1]
                if (confidence > 10):
                    news['Intent'] = intent
                    news['Confidence Level'] = confidence
                else:
                    news = {}  ## Removing news with confidence percentage less than 10%
                    ## Confidence percent threshold needs to be decided
            except:
                news = {}  ## Removing news without intent classification
        return data
    
#     def load(self, days_num = 365):
#         self.logger.info("[Kognetics News Ingestion] Beginning Kognetics News Ingestion")
#         client_ids = self.get_client_ids()
#         list_len = len(client_ids)
                 
# #        client_ids = ['1000527949']#, '1000526584', '1000516254']  ## TRIAL-TEST for AT&T, Comcast and CIENA resp.
#         for ids, entity_id in enumerate(client_ids):
#             permalink = self.permalink_obj.get_permalink(entity_id)

#             if not(permalink == 'no permalink retrieved'):
#                 self.logger.info("[Kognetics News Ingestion] Processing client {} (of {})...".format(ids, list_len))
#                 jdata = self.kognetics_topics_bundle(entity_id, days_num)

#                 jdata['datatype'] = self.kognetics_datatype
#                 jdata['ingestionDate'] = datetime.now().strftime("%Y-%m-%d T%H:%M:%SET")
#                 payload = json.dumps(jdata)
# #                print(payload)  ## TRIAL-TEST to see the final output

#                 file_name = "kognetics_data{}.json".format(ids)

#                 self.kognetics_thread.put_in_queue([payload, file_name])
#                 self.ingest_to_WDS(payload, ids)

#                 self.logger.info("[Kognetics News Ingestion] Client {} Ingestion Complete!".format(entity_id))

#         self.logger.info("[Kognetics News Ingestion] Kognetics News Ingestion Complete!")
#         print("[Kognetics News Ingestion] Complete!")
         
