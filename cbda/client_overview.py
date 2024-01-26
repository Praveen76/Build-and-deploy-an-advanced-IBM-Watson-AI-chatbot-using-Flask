from cbda.cbda_client import CbdaClient
from snp.get_snp_data import GetSnpData


class ClientOverview:
    def __init__(self):
        self.client = CbdaClient()

    # @TODO improve code to place MNEMONICS in a config file thus keep consistency
    # # between snp.get_company_overview(..) and self.get_client_overview(..)
    def get_client_overview(self, entity_id):
        try:
            client_data = self.client.get_client(entity_id)
            if client_data['CapitalIQID']:
                snp = GetSnpData()
                client_overview = snp.get_company_overview(client_data['CapitalIQID'])
                data = [{
                    "company_overview": [
                        {'COMPANY_OVERVIEW_SHORT': client_overview['IQ_SHORT_BUSINESS_DESCRIPTION']},
                        {'COMPANY_OVERVIEW': client_overview['IQ_BUSINESS_DESCRIPTION']}]
                }]
                return data
            return {}
        except KeyError as ke:
            print("Error {}".format(str(ke)))
            print('Key not Retrieved by {}'.format(snp.get_company_overview.__name__))
