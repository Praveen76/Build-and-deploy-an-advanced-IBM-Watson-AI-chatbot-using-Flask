#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced
import json


class OverallRecommendation:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.collection_id = self.wds.get_wds_collection("KCCI-Overall-Recommendation-UAT")

        self.wds = WDSConnectionAdvanced()
        self.collection_id = self.wds.get_wds_collection("KCCI-Overall-Recommendation-UAT")

    def get_overall_recommendation(self, entity_id):
        query = 'datatype::\"Overall\"'

        filter = 'Entity_ID::\"' + entity_id + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.collection_id,
                                                 query = query,
                                                 filter=filter, count=30).get_result()

        countMatches = query_results['matching_results']
        service_data = []

        if countMatches > 0:
            content = query_results['results'][0]
            result = json.loads(content['Content'])
            services = result['Services']

            for item in services:

                evidence = item['Evidence']['EvidenceObjects']
                total_count = 0
                evidence_data = []
                my_dict = {}
                score = 0
                for ev in evidence:
                    try:
                        if ev['ContributionValue'] == 'High' and total_count<6:
                            prev_count = int(my_dict[ev['Type']])
                            score = score + ev['Contribution']
                            ev_type = ev['Type']
                            details = ev['Details']
                            count = int(details[0]['Count'])+ prev_count
                            my_dict[ev['Type']] = count
                            total_count = total_count + 1
                    except KeyError:
                        try:
                            my_dict[ev['Type']] = ev['Details'][0]['Count']
                            score = score + ev['Contribution']
                        except KeyError:
                            my_dict[ev['Type']] = 0
                            score = score + ev['Contribution']
                for key, value in my_dict.items():
                    evidence_data.append({key: str(value)})
                service_data.append({"ServiceName": item['ServiceName'], "ConfidenceValue": item['ConfidenceValue'],
                                     "Score": score, "Evidences": evidence_data})

            temp = {"Services": service_data[0:10]}
            return temp
        return {}
