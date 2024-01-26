from cbda.cbda_client import CbdaClient
from snp.get_snp_data import GetSnpData


class StockTrailing:
    def __init__(self):
        self.client = CbdaClient()

    def get_client_stock_trailing(self, entity_id, months):
        client_data = self.client.get_client(entity_id)
        try:
            if client_data['CapitalIQID']:
                snp = GetSnpData()
                stock_trailing = snp.get_company_stock_trailing(client_data['CapitalIQID'], months)
                return stock_trailing
            return {}
        except KeyError as ke:
            print("Error {}".format(str(ke)))
            print('Key not Retrieved by {}'.format(snp.get_company_stock_trailing.__name__))
            return {}