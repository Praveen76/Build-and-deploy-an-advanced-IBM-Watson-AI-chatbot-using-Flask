#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced
import pandas as pd
import json

class CbdaQRC:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.client_collection_id = self.wds.get_wds_collection("KCCI-QRC")
        self.wds = WDSConnectionAdvanced()
        self.client_collection_id = self.wds.get_wds_collection("KCCI-QRC-UAT")

    def get_qrc_by_name(self, qrc):
        qrc_filter = 'QRC::\"' + qrc + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.client_collection_id,
                                                 filter=qrc_filter, count=1).get_result()

        countMatches = query_results['matching_results']
        qrc_result = {}
        if countMatches > 0:
            result = query_results['results'][0]
            qrc_result = {"QRC": result['QRC'], 'Description': result['Description'],
                          'QRC_Index_Number': result['QRC_Index_Number'],
                          'QRC_Link': result['QRC_Link']}
        return qrc_result

    def get_qrc(self, qrc):
        qrc_filter = 'QRC_Index_Number::\"' + qrc + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                       self.client_collection_id,
                       filter=qrc_filter, count=1).get_result()

        countMatches = query_results['matching_results']
        qrc_result = {}
        if countMatches > 0:
            result = query_results['results'][0]
            qrc_result={"QRC": result['QRC'], 'Description': result['Description'],
                        'QRC_Index_Number': result['QRC_Index_Number'],
                        'QRC_Link': result['QRC_Link']}

        return qrc_result
