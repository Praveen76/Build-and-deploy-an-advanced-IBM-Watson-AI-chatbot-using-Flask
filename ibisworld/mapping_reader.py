from utils.wds_connection_advanced import WDSConnectionAdvanced
import string
import pandas as pd
import json


class MappingReader:
  def __init__(self):
    self.wds = WDSConnectionAdvanced()
    self.collection_id = self.wds.get_wds_collection("KCCI-IBIS-UAT")
    self.keyword_dict = {}

  def load(self):
    PERMITTED_CHARS = string.digits + string.ascii_letters + "., "

    xls = pd.ExcelFile('IBIS_cbda_industry_MappingV2.xlsx')

    for i in xls.sheet_names:
      df = xls.parse(i)

      data = df.to_dict(orient='records')
      row_num = 0
      for index, entry in df.iterrows():
        jfile = "ibis_naics_mapping"+str(row_num) + ".json"
        try:
          temp= {'Code': entry['Code'], 'Title': entry['Title'], 'LOB_Name': entry['LOB_Name'], 'Sector': entry['Sector'],
                 'Sector_Name': entry['Sector_Name'], 'Segment': entry['Segment'], 'Segment_Name': entry['Segment_Name']}
          doc_id = self.wds.load_json_wds(temp, jfile, self.collection_id)["document_id"]
        except Exception as e:
          print(e)
        row_num = row_num + 1

ibis = MappingReader()
ibis.load()
