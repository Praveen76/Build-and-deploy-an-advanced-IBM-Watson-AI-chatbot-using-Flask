from cbda.kpi_measure_calculator import KPIMeasureCalculator
from cbda.client_entity import ClientEntity


class ApiWorker:

    def __init__(self):
        self.client_entity = ClientEntity()

        self.KPI_RATIOS_AND_MEASURES = [
            ['ASSET_EFFICIENCY', ['FIXED_ASSET_UTILIZATION', 'GMROI', 'RETURN_ON_ASSETS']],
            ['CASH_MANAGEMENT',
             ['CAPEX_COVERAGE', 'CASH_FROM_OPERATIONS_BY_REVENUE', 'CF_TO_DEBT', 'FREE_CASH_FLOW_PER_REVENUE',
              'DEBT_TO_CAPITAL_RATIO']],
            ['COST_MANAGEMENT',
             ['COGS_PER_REVENUE', 'RnD_PER_REVENUE', 'SGnA_PER_REVENUE', 'OPERATING_EXPENSE_MARGIN',
              'EXPENSES_TO_ASSETS']],
            ['PROFITABILITY',
             ['EBITDA_MARGIN', 'GROSS_PROFIT_MARGIN', 'NET_INCOME_MARGIN', 'RETURN_ON_EQUITY', 'OPERATING_MARGIN']],
            ['SIZE_AND_GROWTH', ['TOTAL_REVENUE_GROWTH_1YR', 'CAPITAL_EXPENDITURE_GROWTH_1YR']]
        ]

    def make_peer_comparison_json(self, peer_calculation_data, entityId):

        peer_comparison_dict = {'ResponseStatus': {"StatusCode": 200, "StatusDetail": "Success"}}
        peer_graph_array = []
        summary_array = []
        for data in peer_calculation_data:
            Data_Array = []
            for measure, ratioKeys in self.KPI_RATIOS_AND_MEASURES:
                Data_Array.append({
                    'Measure': " ".join(measure.title().split("_")),
                    'Percent': data[measure],
                    'kpi': [{
                            'kpiname': " ".join(key.title().split("_")),
                            'kpipercent': float(data[key]) if (data[key] != '*') else data[key]
                            } for key in ratioKeys]
                    })
            peer_graph_array.append(self.create_peer_graph_data_object(entityId, data, Data_Array))
            summary_array.append(self.create_summary_object(entityId, data))

        revised_peer_graph = []
        revised_summary = []
        found = False
        for index, item in enumerate(peer_graph_array):
                if index < 5:
                    if item['CompanyId'] == str(entityId):
                        found = True
                    revised_peer_graph.append(item)
                else:
                    if found == True:
                        break
                    if item['CompanyId'] == str(entityId):
                        found=True
                        revised_peer_graph.append(item)
                        break
        found = False
        for index, item in enumerate(summary_array):
                if index < 5:
                    if item['CompanyId'] == str(entityId):
                        found = True
                    revised_summary.append(item)
                else:
                    if found == True:
                        break
                    if item['CompanyId'] == str(entityId):
                        found=True
                        revised_summary.append(item)
                        break
        peer_comparison_dict['peerGraphData'] = revised_peer_graph
        peer_comparison_dict['summaryDetails'] = revised_summary

        return peer_comparison_dict

    def check_and_set_client_info(self, entityId, data):
        if data['MPPClient'] == data['IQ_COMPANY_NAME']:
            return [entityId, 'true']
        else:
            return [data['IQ_COMPANY_ID'], 'false']

    def create_peer_graph_data_object(self, entityId, data, Data_Array):
        client_id_and_flag = self.check_and_set_client_info(entityId, data)

        peer_graph_dict = {
            'CompanyName': data['IQ_COMPANY_NAME'],
            'CompanyId': client_id_and_flag[0],
            'isCurrent': client_id_and_flag[1],
            'Data': Data_Array
        }
        return peer_graph_dict

    def create_summary_object(self, entityId, data):
        client_id_and_flag = self.check_and_set_client_info(entityId, data)

        summary_dict = {
            'CompanyId': client_id_and_flag[0],
            'isCurrent': client_id_and_flag[1],
            'EnterpriseId': "",
            'CompanyName': data['IQ_COMPANY_NAME'],
            'OverallPercent': data['OverallPercent'],
            'Size': "*" if data['TOTAL_REVENUE_USD'] == "*" else round(float(data['TOTAL_REVENUE_USD']) / 1000, 4)
        }

        return summary_dict

    def get_peer_comparison(self, clientId):

        resolved_id = self.client_entity.entity_resolution(clientId)

        entityId = resolved_id[0]

        kpi_measure_calc = KPIMeasureCalculator()
        peer_calculation_results = kpi_measure_calc.get_peer_comparison_data(entityId)

        if peer_calculation_results:
            peerData = self.make_peer_comparison_json(peer_calculation_results, entityId)
            return peerData
        else:
            return {}
