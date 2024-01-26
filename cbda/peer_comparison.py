#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced
import json
from cbda.api_worker import ApiWorker

class PeerComparison:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.peer_collection_id = self.wds.get_wds_collection("KCCI-Peer-Comparison")
        # self.client_collection_id = self.wds.get_wds_collection("KCCI-CLIENT")

        self.wds = WDSConnectionAdvanced()
        self.peer_collection_id = self.wds.get_wds_collection("KCCI-Peer-Comparison-UAT")
        self.client_collection_id = self.wds.get_wds_collection("KCCI-CLIENT-UAT")

    def get_peer_data(self, entity_id):
        peerCompApi = ApiWorker()
        my_data = peerCompApi.get_peer_comparison(entity_id)
        return my_data['peerGraphData'], 200

    ##based on the capitalIQID to get the peer collection
    def get_peercomparison(self, entity_id):
        query = 'datatype::\"ClientAndPeerRatios\"'

        filter = 'Entity_ID::\"' + entity_id + '\"'

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.peer_collection_id,
                                                 query = query,
                                                 filter=filter, count=1).get_result()

        countMatches = query_results['matching_results']
        data = {}
        if countMatches > 0:
            content = query_results['results'][0]
            result = json.loads(content['Content'])
            data = {

                    "Company_name": result['IQ_COMPANY_NAME'],
                    "FIXED_ASSET_UTILIZATION": result['FIXED_ASSET_UTILIZATION'], 'GMROI': result['GMROI'],
                    'RETURN_ON_ASSETS': result['RETURN_ON_ASSETS'], 'CAPEX_COVERAGE': result['CAPEX_COVERAGE'],
                    'CASH_FROM_OPERATIONS_BY_REVENUE': result['CASH_FROM_OPERATIONS_BY_REVENUE'],
                    'CF_TO_DEBT': result['CF_TO_DEBT'],
                    'FREE_CASH_FLOW_PER_REVENUE': result['FREE_CASH_FLOW_PER_REVENUE'],
                    'DEBT_TO_CAPITAL_RATIO': result['DEBT_TO_CAPITAL_RATIO'],
                    'COGS_PER_REVENUE': result['COGS_PER_REVENUE'],
                    'RnD_PER_REVENUE': result['RnD_PER_REVENUE'],
                    'OPERATING_EXPENSE_MARGIN': result['OPERATING_EXPENSE_MARGIN'],
                    'SGnA_PER_REVENUE': result['SGnA_PER_REVENUE'],
                    'EXPENSES_TO_ASSETS': result['EXPENSES_TO_ASSETS'],
                    'EBITDA_MARGIN': result['EBITDA_MARGIN'],
                    'GROSS_PROFIT_MARGIN': result['GROSS_PROFIT_MARGIN'],
                    'NET_INCOME_MARGIN': result['NET_INCOME_MARGIN'],
                    'RETURN_ON_EQUITY': result['RETURN_ON_EQUITY'],
                    'OPERATING_MARGIN': result['OPERATING_MARGIN'],
                    'TOTAL_REVENUE_GROWTH_1YR': result['TOTAL_REVENUE_GROWTH_1YR'],
                    'CAPITAL_EXPENDITURE_GROWTH_1YR': result['CAPITAL_EXPENDITURE_GROWTH_1YR'],
                    'TOTAL_REVENUE_USD': result['TOTAL_REVENUE_USD']
                    }
        return data
    def get_peers(self, entity_id):
        query = 'datatype::\"PeerList\"'
        filter = 'Entity_ID::\"' + entity_id + '\"'
        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.peer_collection_id,
                                                 query=query,
                                                 filter=filter, count=1).get_result()
        countMatches = query_results['matching_results']
        my_dict = {}
        results = []
        if countMatches > 0:
            for item in query_results['results']:
                result = json.loads(item['Content'])
                try:
                    for i in range(1, 11):
                        id = "peer"+ str(i)+ "_id"
                        my_dict[id] = result[id]
                        data = self.get_peercomparison(result[id])
                        results.append(data)
                except KeyError:
                    print("No data")
        return results

    def get_peers_by_id(self, peer_id):
        query = 'datatype::\"ClientAndPeerRatios\"'
        filter = 'Entity_ID::\"' + peer_id + '\"'
        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.peer_collection_id,
                                                 query=query,
                                                 filter=filter, count=1).get_result()

        countMatches = query_results['matching_results']
        data = {}
        if countMatches > 0:
            content = query_results['results'][0]
            result = json.loads(content['Content'])
            data = {"FIXED_ASSET_UTILIZATION": result['FIXED_ASSET_UTILIZATION'], 'GMROI': result['GMROI'],
                    'RETURN_ON_ASSETS': result['RETURN_ON_ASSETS'], 'CAPEX_COVERAGE': result['CAPEX_COVERAGE'],
                    'CASH_FROM_OPERATIONS_BY_REVENUE': result['CASH_FROM_OPERATIONS_BY_REVENUE'],
                    'CF_TO_DEBT': result['CF_TO_DEBT'],
                    'FREE_CASH_FLOW_PER_REVENUE': result['FREE_CASH_FLOW_PER_REVENUE'],
                    'DEBT_TO_CAPITAL_RATIO': result['DEBT_TO_CAPITAL_RATIO'],
                    'COGS_PER_REVENUE': result['COGS_PER_REVENUE'],
                    'RnD_PER_REVENUE': result['RnD_PER_REVENUE'],
                    'OPERATING_EXPENSE_MARGIN': result['OPERATING_EXPENSE_MARGIN'],
                    'SGnA_PER_REVENUE': result['SGnA_PER_REVENUE'],
                    'EXPENSES_TO_ASSETS': result['EXPENSES_TO_ASSETS'],
                    'EBITDA_MARGIN': result['EBITDA_MARGIN'],
                    'GROSS_PROFIT_MARGIN': result['GROSS_PROFIT_MARGIN'],
                    'NET_INCOME_MARGIN': result['NET_INCOME_MARGIN'],
                    'RETURN_ON_EQUITY': result['RETURN_ON_EQUITY'],
                    'OPERATING_MARGIN': result['OPERATING_MARGIN'],
                    'TOTAL_REVENUE_GROWTH_1YR': result['TOTAL_REVENUE_GROWTH_1YR'],
                    'CAPITAL_EXPENDITURE_GROWTH_1YR': result['CAPITAL_EXPENDITURE_GROWTH_1YR'],
                    'TOTAL_REVENUE_USD': result['TOTAL_REVENUE_USD']
                    }
        return data
