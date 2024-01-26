from cbda.cbda_client import CbdaClient
from snp.get_snp_data import GetSnpData
import re

class NAICS:
    def __init__(self):
        self.client = CbdaClient()

    def get_NAICS_code(self, entity_id):
        client_data = self.client.get_client(entity_id)
        try:
            if client_data['CapitalIQID']:
                snp = GetSnpData()
                naics_info = snp.get_NAICS_code(client_data['CapitalIQID'])
                naics = re.findall(r'\d+', naics_info['IQ_BUS_SEG_NAIC'])
                return naics[0]
            return {}
        except KeyError as ke:
            print("Error {}".format(str(ke)))
            print('Key not Retrieved by {}'.format(snp.get_company_stock_trailing.__name__))
            return {}