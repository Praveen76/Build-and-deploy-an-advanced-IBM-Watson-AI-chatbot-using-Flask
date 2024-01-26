#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced
import json


class ClientNews:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.collection_id = self.wds.get_wds_collection("KCCI-News-Analysis")

        self.wds = WDSConnectionAdvanced()
        self.collection_id = self.wds.get_wds_collection("KCCI-News-Analysis-UAT")

    def get_news_chart(self, entity_id):
        query = 'datatype::\"chart\"'
        filter = 'Entity_ID::\"' + entity_id + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.collection_id,
                                                 query=query,
                                                 filter=filter, count=1).get_result()

        countMatches = query_results['matching_results']
        data = {}
        if countMatches > 0:
            data = query_results['results'][0]['Content']
            data = json.loads(data)
        return data

    def get_news(self, entity_id):
        query = 'datatype::\"chart\"'
        filter = 'Entity_ID::\"' + entity_id + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.collection_id,
                                                 query=query,
                                                 filter=filter, count=1).get_result()

        countMatches = query_results['matching_results']
        data = []
        my_dict = {}
        if countMatches > 0:
            for item in query_results['results']:
                result = json.loads(item['Content'])
                try:
                    temp = result['Data']
                    for i in temp:
                        details = i['Details']
                        for detail in details:

                            try:
                                my_count = my_dict[detail['DisplayL2Name']]
                                count = int(my_count) + int(detail['Count'])
                                my_dict[detail['DisplayL2Name']] = str(count)
                            except KeyError:

                                my_dict[detail['DisplayL2Name']] = detail['Count']
                except KeyError:
                    print("No data")
        for key, value in my_dict.items():
            temp = {'DisplayL2Name': key, 'Count': value}
            data.append(temp)

        final_data = sorted(data, key=lambda k: int(k.get('Count', 0)), reverse=True)
        return final_data[0:10]

