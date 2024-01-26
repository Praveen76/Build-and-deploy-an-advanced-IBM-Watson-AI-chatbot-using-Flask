#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced
from cbda.peer_comparison import PeerComparison
class CbdaClient:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.client_collection_id = self.wds.get_wds_collection("KCCI-CLIENT")
        self.wds = WDSConnectionAdvanced()
        self.client_collection_id = self.wds.get_wds_collection("KCCI-CLIENT-UAT")

    def get_client_capiqid(self, entity_id):
        filter = 'EntityID::\"' + str(entity_id) + '\", datatype:\"KCCI-CLIENT\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.client_collection_id,
                                                 filter=filter, count=1).get_result()

        if len(query_results['results'])>0:
            return query_results['results'][0]['CapitalIQID']
        return None

    def get_client_industry(self, entity_id):
        filter = 'EntityID::\"' + str(entity_id) + '\", datatype:\"KCCI-CLIENT\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.client_collection_id,
                                                 filter=filter, count=1).get_result()

        if len(query_results['results'])>0:
            return query_results['results'][0]
        return None

    def get_client(self, entity_id):
        filter = 'EntityID::\"' + entity_id + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.client_collection_id,
                                                 filter=filter, count=1).get_result()

        countMatches = query_results['matching_results']
        data = {}
        if countMatches > 0:
            result = query_results['results'][0]
            data = {"CompanyName": result['CompanyName'], 'TRTicker': result['TRTicker'],
                          'EmployeeTotal': result['EmployeeTotal'], 'Rvenue_2007': result['Revenue-FY2017'],
                          'OP_Income_FY2017': result['OP-Income-FY2017'],
                          'NetIncome_FY2017': result['NetIncome-FY2017'],
                          'TotalAssets_2017': result['TotalAssets-2017'],
                          'Total_Equity_2017': result['Total-Equity-2017'],
                          'CapitalIQID': result['CapitalIQID']
                    }

            if result['CapitalIQID']:
                peer = PeerComparison()
                kpi = peer.get_peercomparison(result['CapitalIQID'])
                context = data.copy()
                context.update(kpi)
                return context
        return data
    
    def get_entityid(self, company_name):
        filter = 'CompanyName:\"' + str(company_name) + '\", datatype:\"KCCI-CLIENT\"'
        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.client_collection_id,
                                                 filter=filter, count=1).get_result()
        if len(query_results['results']) > 0:
            print(query_results['results'][0]['EntityID'])
            return query_results['results'][0]['EntityID']
        else:
            return None

    def get_All_Client(self):
        filter = 'datatype:\"KCCI-CLIENT\"'
        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.client_collection_id,
                                                 filter=filter,
                                                 count=10000).get_result()
        filter_result = query_results['results']
        return filter_result
