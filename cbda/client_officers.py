from cbda.cbda_client import CbdaClient
from snp.get_snp_data import GetSnpData


class ClientOfficers:
    def __init__(self):
        self.client = CbdaClient()

    # @TODO improve code to place MNEMONICS in a config file thus keep consistency
    # # between snp.get_company_highlights(..) and self.get_client_highlights(..)
    def get_client_officers(self, entity_id):
        client_data = self.client.get_client(entity_id)
        try:
            if client_data['CapitalIQID']:
                snp = GetSnpData()
                client_officers = snp.get_client_officers(client_data['CapitalIQID'])
                data = []
                counter = 0
                for client_officer in client_officers[0]:
                    data1 = {"Client_Name": client_officer, "Client_Designation": client_officers[49][counter], "First_Relationship": client_officers[62][counter] }
                    data.append(data1)
                    counter = counter+1
                return data
            return {}
        except KeyError as ke:
            print("Error {}".format(str(ke)))
            print('Key not Retrieved by {}'.format(snp.get_client_officers.__name__))
            return {}