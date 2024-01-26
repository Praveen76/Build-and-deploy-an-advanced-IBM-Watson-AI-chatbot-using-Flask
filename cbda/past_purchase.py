#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced
import json

class PastPurchase:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.pp_collection_id = self.wds.get_wds_collection("KCCI-PP")

        self.wds = WDSConnectionAdvanced()
        self.pp_collection_id = self.wds.get_wds_collection("KCCI-PP-UAT")

    def get_past_purchases(self, entity_id):
        qrc_identifiers = self.__get_qrc_identifiers(entity_id)
        return self.__get_qrc_service_name(qrc_identifiers)

    ##Get past purchases for the given client the entity/client_id from PP collection
    def __get_qrc_service_name(self, qrc_identifiers):
        if len(qrc_identifiers)==0:
            data = {
              "past_purchase_services": []
            }
            return data
        qrc_services = list()
        data = {}
        last_idx = len(qrc_identifiers) - 1
        filter = 'datatype::"service",('
        for idx,qrc_id in enumerate(qrc_identifiers):
            if not idx == last_idx:
                filter+= 'identifier::"' + qrc_id + '"|'
            else:
                filter+= 'identifier::"' + qrc_id + '")'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.pp_collection_id,
                                                 filter=filter,
                                                 return_fields='DisplayName',
                                                 count=100).get_result()
        countMatches = query_results['matching_results']
        if countMatches > 0:
            results = query_results['results']
            for item in results:
                if item['DisplayName']:
                    qrc_services.append(item['DisplayName'])

        if len(qrc_services)>0:
            data = {
                "past_purchase_services":qrc_services
            }

        return data


    def __get_qrc_identifiers(self, entity_id):
        filter = 'datatype::"edge",client_identifier::\"' + entity_id + '\"'
        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.pp_collection_id,
                                                 filter=filter,
                                                 return_fields='qrc_identifier',
                                                 count = 100).get_result()

        countMatches = query_results['matching_results']
        qrc_identifiers = list()
        if countMatches > 0:
            results = query_results['results']
            for item in results:
                if item['qrc_identifier']:
                    qrc_identifiers.append(item['qrc_identifier'])
        return qrc_identifiers
