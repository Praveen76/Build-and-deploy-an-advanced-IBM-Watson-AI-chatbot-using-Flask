from cbda.cbda_client import CbdaClient
from snp.get_snp_data import GetSnpData


class ClientHighlights:
    def __init__(self):
        self.client = CbdaClient()

    # @TODO improve code to place MNEMONICS in a config file thus keep consistency
    # # between snp.get_company_highlights(..) and self.get_client_highlights(..)
    def get_client_highlights(self, entity_id):
        client_data = self.client.get_client(entity_id)
        try:
            if client_data['CapitalIQID']:
                snp = GetSnpData()
                client_highlights = snp.get_company_highlights(client_data['CapitalIQID'])
                public = "Yes" if client_highlights['IQ_EQUITY_LIST'] or client_highlights['IQ_EXCHANGE'] else 'No'
                data = [{
                    "company_highlights": [
                    {'COMPANY_ADDRESS': client_highlights['IQ_COMPANY_ADDRESS']},
                    {'COMPANY_NAME': client_highlights['IQ_COMPANY_NAME']},
                    {'COMPANY_NAME_LONG': client_highlights['IQ_COMPANY_NAME_LONG']},
                    {'COMPANY_PHONE': client_highlights['IQ_COMPANY_PHONE']},
                    {'COMPANY_WEBSITE': client_highlights['IQ_COMPANY_WEBSITE']},
                    {'COMPANY_MAIN_FAX': client_highlights['IQ_COMPANY_MAIN_FAX']},
                    {'IS_PUBLIC': public}]
                }]
                return data
            return {}
        except KeyError as ke:
            print("Error {}".format(str(ke)))
            print('Key not Retrieved by {}'.format(snp.get_company_highlights.__name__))
            return {}