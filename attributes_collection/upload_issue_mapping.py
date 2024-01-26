import os
import pandas as pd
import json
from utils.wds_connection_advanced import WDSConnectionAdvanced

file_name = "SAM-AI_Issues_Tracking-3-21-2019 with topics.xlsx"

class UploadIssueMapping():
    def __init__(self):
        self.wds = WDSConnectionAdvanced()
        self.collection_id = self.wds.get_wds_collection('KCCI-Topic-Mapping')

    def upload_mapping(self, file_name):
        try:
            issue_data_df = pd.read_excel(file_name, sheet_name='Final_Issue_Taxonomy')
            issue_data_df = issue_data_df.set_index('Issue_Mapping_ID')
            row_num = 0
            for index, issue_frame in issue_data_df.iterrows():
                jfile = "issue_mapping" + str(row_num) + ".json"
                try:
                    load_data = {'Intents_L1_Label': issue_frame['Intents_L1_Label'],
                                 'Intents_L2_Label': issue_frame['Intents_L2_Label'],
                                 'WA_Intent_Label': issue_frame['WA_Intent_Label'],
                                 'SAM_AI_UI_Label': issue_frame['SAM_AI_UI_Label'],
                                 'SAM_AI_News_Label': issue_frame['SAM_AI_News_Label'],
                                 'AQL_Required': issue_frame['AQL_Required']
                                 }
                    doc_id = self.wds.load_json_wds(json.dumps(load_data), jfile, self.collection_id)["document_id"]
                except Exception as ex:
                    print(ex)
                finally:
                    row_num += 1
            return "Finished"
        except Exception as ex:
            return ex

issue_map = UploadIssueMapping()
issue_map.upload_mapping(file_name)
