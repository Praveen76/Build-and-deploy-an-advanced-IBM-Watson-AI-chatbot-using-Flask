import logging
import os
import pandas as pd
import json
import datetime
import time
from utils.wds_connection_advanced import WDSConnectionAdvanced

class EikonMappingLoading:
    def __init__(self):
        self.logger = logging.getLogger()
        self.wds = WDSConnectionAdvanced()
        self.collection_id = self.wds.get_wds_collection('KCCI-EIKON')

    def load(self, file_name='cbda-Eikon-Mapping.xlsx'):
        doc_with_error = 0

        submitted_documents = []  # A list of Document objects to be verified and checked after ingestion
        xls = pd.ExcelFile(file_name)

        for i in xls.sheet_names:
            df = xls.parse(i)

            df['Date'] = str(datetime.datetime.now())
            df1 = df.where((pd.notnull(df)), None)
            data = df1.to_dict(orient='records')
            row_num = 0
            for entry in data:
                jfile = str(row_num)+ ".json"
                try:
                    data = {'cbda_CompanyName': entry['cbda_CompanyName'],
                            'Entity_ID': str(entry['Entity_ID']),
                            'cbda_CleanName': entry['cbda_CleanName'],
                            'RIC': entry['Eikon_RIC']}
                    doc_id = self.wds.load_json_wds(data, jfile, self.collection_id)["document_id"]

                except Exception as e:
                    self.logger.error("Error in file name %s row number %i" %(file_name, row_num))
                    self.logger.error("Problem in loading Eikon Mapping Data", json.dumps(entry, indent=2))

                    doc_with_error = doc_with_error + 1
                row_num = row_num + 1

load = EikonMappingLoading()
load.load()
