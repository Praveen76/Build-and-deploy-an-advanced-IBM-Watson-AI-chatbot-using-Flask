import numpy as np
import json
#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced

class KPIMeasureCalculator:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        # self.wds = WDSConnection()
        # self.environment = self.wds.environment_id
        self.wds = WDSConnectionAdvanced()
        self.environment = self.wds.environment_id
        try:
            self.peer_comp_collection_id = self.wds.get_wds_collection('KCCI-Peer-Comparison-UAT')
        except TypeError:
            print("Collection {} does not exist".format('KCCI-Peer-Comparison-UAT'))

        self.peer_list_datatype = 'PeerList'
        self.ratios_datatype = 'ClientAndPeerRatios'

        self.MEASURES = ['ASSET_EFFICIENCY', 'CASH_MANAGEMENT', 'COST_MANAGEMENT', 'PROFITABILITY', 'SIZE_AND_GROWTH']

        self.KPI_MEASURES = ['FIXED_ASSET_UTILIZATION', 'GMROI', 'RETURN_ON_ASSETS', 'CAPEX_COVERAGE',
                             'CASH_FROM_OPERATIONS_BY_REVENUE', 'CF_TO_DEBT', 'FREE_CASH_FLOW_PER_REVENUE',
                             'DEBT_TO_CAPITAL_RATIO', 'COGS_PER_REVENUE', 'RnD_PER_REVENUE', 'SGnA_PER_REVENUE',
                             'OPERATING_EXPENSE_MARGIN', 'EXPENSES_TO_ASSETS', 'EBITDA_MARGIN', 'GROSS_PROFIT_MARGIN',
                             'NET_INCOME_MARGIN', 'RETURN_ON_EQUITY', 'OPERATING_MARGIN',
                             'TOTAL_REVENUE_GROWTH_1YR', 'CAPITAL_EXPENDITURE_GROWTH_1YR']

        self.lowering_kpi_list = ['DEBT_TO_CAPITAL_RATIO', 'COGS_PER_REVENUE', 'SGnA_PER_REVENUE',
                                  'OPERATING_EXPENSE_MARGIN', 'EXPENSES_TO_ASSETS']

    def get_mean(self, keys, pRatio):
        ratioArr = []
        for k in keys:
            if pRatio[k] != '*' and pRatio[k] != 'NaN' and pRatio[k] != 'nan' and pRatio[k] != 'NM':
                ratioArr.append(float(pRatio[k]))

        if ratioArr is not None and len(ratioArr) > 0:
            return np.sum(ratioArr) / len(keys)
        else:
            return "*"

    def get_kpi_measures(self, peerRatioDict, pRatio):
        peerRatioDict['ASSET_EFFICIENCY'] = self.get_mean(['FIXED_ASSET_UTILIZATION', 'GMROI', 'RETURN_ON_ASSETS'],
                                                          pRatio)
        peerRatioDict['CASH_MANAGEMENT'] = self.get_mean(
            ['CAPEX_COVERAGE', 'CASH_FROM_OPERATIONS_BY_REVENUE', 'CF_TO_DEBT', 'FREE_CASH_FLOW_PER_REVENUE',
             'DEBT_TO_CAPITAL_RATIO'], pRatio)
        peerRatioDict['COST_MANAGEMENT'] = self.get_mean(
            ['COGS_PER_REVENUE', 'RnD_PER_REVENUE', 'SGnA_PER_REVENUE', 'OPERATING_EXPENSE_MARGIN',
             'EXPENSES_TO_ASSETS'],
            pRatio)
        peerRatioDict['PROFITABILITY'] = self.get_mean(
            ['EBITDA_MARGIN', 'GROSS_PROFIT_MARGIN', 'NET_INCOME_MARGIN', 'RETURN_ON_EQUITY', 'OPERATING_MARGIN'],
            pRatio)
        peerRatioDict['SIZE_AND_GROWTH'] = self.get_mean(['TOTAL_REVENUE_GROWTH_1YR', 'CAPITAL_EXPENDITURE_GROWTH_1YR'],
                                                         pRatio)

        return peerRatioDict

    def find_outliers(self, measure, key, peerRatios):
        logMeasure = [np.log10(m + 1 - min(m, 0.0001)) for m in measure if m != "*"]
        logMeasureAsterik = ["*" if m == "*" else np.log10(m + 1 - min(m, 0.0001)) for m in measure]

        peerRatiosArr = []

        for m, ratioDict in zip(logMeasureAsterik, peerRatios):
            if m == "*":
                ratioDict[key + '_FLAG'] = "*"
                peerRatiosArr.append(ratioDict)
            else:
                _max, _min = self.get_iqr(logMeasure)
                if m < _min:
                    ratioDict[key + '_FLAG'] = str(-1)
                elif m > _max:
                    ratioDict[key + '_FLAG'] = str(1)
                else:
                    ratioDict[key + '_FLAG'] = str(0)

                peerRatiosArr.append(ratioDict)

        return peerRatiosArr

    def get_iqr(self, logMeasure):
        q75, q25 = np.percentile(logMeasure, [75, 25])
        iqr = q75 - q25
        _min = q25 - (iqr * 1.5)
        _max = q75 + (iqr * 1.5)
        return _max, _min

    def reverse_kpis_and_flags(self, normalized_group_ratios):
        """
        Lower the better KPIs here are referenced as lowering_kpis. This function reverses a normalized ratio by
        subtracting the ratio by 100. i.e if X is the ratio value, it's reverse becomes (100 -  X). Here we also
        switch the outlier flags 1 and -1 to -1 and 1 respectively.
        :param normalizedGroupRatios: KPI ratios with values between [0-100] across the group.
        :return: Group ratios with reversed values for certain KPIs and it's corresponding flags
        """

        reversed_lowering_kpi_group_ratios = []
        for i, normRatioDict in enumerate(normalized_group_ratios):
            for key in self.lowering_kpi_list:

                if "*" in (normRatioDict[key], normRatioDict[key + '_FLAG']):
                    continue
                else:
                    normRatioDict[key] = (100 - float(normRatioDict[key]))
                    normRatioDict[key + '_FLAG'] = str(int(normRatioDict[key + '_FLAG']) * (-1))

            reversed_lowering_kpi_group_ratios.append(normRatioDict)

        return reversed_lowering_kpi_group_ratios

    def normalize_measures(self, peerRatios):

        for i, key in enumerate(self.KPI_MEASURES):
            ratioArr = []
            for ratioDict in peerRatios:
                currVal = "*" if ratioDict[key] == "*" else float(ratioDict[key])
                ratioDict[key] = currVal
                ratioArr.append(ratioDict[key])

            peerRatios = self.find_outliers(ratioArr, key, peerRatios)

        return peerRatios

    def normalize_peer_measures(self, peerGroupRatios):
        """
        This method is responsible for normalizing the 5 Measures values between [0-100].
        param: Takes peer group ratios array of dictionaries consisting of the 5 measures.
        output: array of dictionaries with normalized measures.
        """

        peer_ratio_group_dict_list = []

        for i, key in enumerate(self.KPI_MEASURES):
            '''
            If the flag is 0, we simply add it to the ratioArr list and use that later for normalization.
            If the flag is "*", set the value to "*". // This may change.
            If the flag is 1, set the value to 100. 
            If the flag is -1, set the value to 0. 
            We normalize the group ratios by the following formula: here group_values are with flag 0 only. 
            normalized_value = (original_value - min(group_values)) / (max(group_values) - min(group_values))*100 
            '''

            ratioArr = []
            for peerRatioDict in peerGroupRatios:
                if peerRatioDict[key + '_FLAG'] == "*":
                    continue
                else:
                    if int(peerRatioDict[key + '_FLAG']) == 0:
                        ratioArr.append(float(peerRatioDict[key]))
                    else:
                        continue

            for peerRatioDict in peerGroupRatios:
                # Normalizing the measures between [0 - 100].
                if str(peerRatioDict[key + '_FLAG']) == "*":
                    peerRatioDict[key] = "*"
                else:
                    if int(peerRatioDict[key + '_FLAG']) == -1:
                        peerRatioDict[key] = float(0.0)
                    elif int(peerRatioDict[key + '_FLAG']) == 1:
                        peerRatioDict[key] = float(100.0)
                    else:
                        try:
                            if len(ratioArr) > 1:
                                peerRatioDict[key] = ((float(peerRatioDict[key]) - np.min(ratioArr)) / (
                                        np.max(ratioArr) - np.min(ratioArr))) * 100
                        except Exception:
                            print("[KPI Recommendation] Unexpected error occurred while normalizing the ratios: {error}")

                # Add to the group array all the peer dictionaries.
                peer_ratio_group_dict_list.append(peerRatioDict)

        unique_peer_ratio_arr = self.remove_duplicate_peer_dict(peer_ratio_group_dict_list)

        return unique_peer_ratio_arr

    def remove_duplicate_peer_dict(self, peer_ratios_group_dict):
        # I added 5 times the same dictionary so filtering Unique dictionaries.
        unique_peer_ratios_arr = []
        for r in peer_ratios_group_dict:
            if r not in unique_peer_ratios_arr:
                unique_peer_ratios_arr.append(r)
        return unique_peer_ratios_arr

    def add_overall_percent(self, peer_ratios_group_arr):
        peer_ratios_with_overall = []
        for normDict in peer_ratios_group_arr:
            measureList = []
            for m in self.MEASURES:
                if normDict[m] != "*":
                    measureList.append(normDict[m])
                else:
                    continue
            if measureList is not None and len(measureList) > 0:
                normDict['OverallPercent'] = np.mean(measureList)
            else:
                normDict['OverallPercent'] = "*"
            peer_ratios_with_overall.append(normDict)

        return peer_ratios_with_overall

    def handle_astericks_in_measures(self, peer_ratios_group_arr):
        peer_ratios_with_default_zero = []
        for ratioDict in peer_ratios_group_arr:
            private_company_check = []
            for m in self.MEASURES:
                if ratioDict[m] == "*":
                    private_company_check.append(ratioDict[m])
                    ratioDict[m] = 0.0

            if private_company_check is not None and len(private_company_check) == len(self.MEASURES):
                continue
            else:
                peer_ratios_with_default_zero.append(ratioDict)
        return peer_ratios_with_default_zero

    def calculate_measures_from_normalized_kpis(self, adjusted_norm_group_ratios):
        """
        This method calculated the 5 measures from normalized KPI values. It also adds Overall percent.
        :param adjusted_norm_group_ratios: normalized and reversed KPI values for peers in a list of dictionaries.
        :return: List of dictionaries with added measures and overall percent.
        """
        group_ratios_with_measures = []
        for norm_dict in adjusted_norm_group_ratios:
            norm_dict_copy = {key: value for key, value in norm_dict.items()}

            added_measures_dict = self.get_kpi_measures(norm_dict, norm_dict_copy)
            group_ratios_with_measures.append(added_measures_dict)

        group_ratios_with_overall_percent = self.add_overall_percent(group_ratios_with_measures)

        return group_ratios_with_overall_percent

    def prepare_json_for_api_worker(self, raw_kpi_dict_list, peer_group_ratios_with_measures):

        peer_comparison_response_array = []
        peer_comp_dict = {}

        assert len(raw_kpi_dict_list) == len(peer_group_ratios_with_measures)
        for raw_kpi_dict in raw_kpi_dict_list:
            peer_comp_dict = {key: value for key, value in raw_kpi_dict.items()}
            filtered_item = list(filter(
                    lambda x: x['IQ_COMPANY_ID'] == peer_comp_dict['IQ_COMPANY_ID'], peer_group_ratios_with_measures
                ))[0]
            for m in self.MEASURES:
                peer_comp_dict[m] = filtered_item[m]
            peer_comp_dict.update({'OverallPercent': filtered_item['OverallPercent']})
            peer_comparison_response_array.append(peer_comp_dict)

        return self.handle_astericks_in_measures(peer_comparison_response_array)

    def get_peer_comparison_data(self, entityid):
        """
        This is the entry point for calculating the Peer comparison results. This method reads in Peers list for
        an entityId and ratios for those Peers from peers data from S&P. Once we have an array of dictionaries with
        all the peer ratios, we normalize the KPIs. We fixed the KPI values that have better performance when they are
        lower, we call it reversing those KPIs. Then, we calculate and add the 5 measures and an overallPercent to the
        dictionaries.
        :param entityid: Entity id of a company
        :return: dictionary consisting of all the peer ratios and measures for a company.
        """

        peerGroupRatios = []
        rawKpiValuesCopy = []

        peer_list_response = self.wds.discovery.query(self.environment,
                                            self.peer_comp_collection_id,
                                            query='datatype::"{}"'.format(self.peer_list_datatype),
                                            filter='Entity_ID::"{}"'.format(entityid),
                                            count=1).get_result()
        peerList = peer_list_response['results']

        if peerList:
            peersData = json.loads(peerList[0]["Content"])

            peerIdList = [peersData['CapId']]
            for i in range(1, 11):
                peerIdList.append(peersData['peer' + str(i) + '_id'])

            filter_string = " ".join([_peerId for _peerId in peerIdList if _peerId != "*"])
            peer_ratios_list = self.wds.discovery.query(self.environment,
                                              self.peer_comp_collection_id,
                                              filter='datatype::"{}"'.format(self.ratios_datatype),
                                              query=filter_string
                                              ).get_result()

            for peerRatios in peer_ratios_list['results']:
                peerRatioData = None
                peerRatioDict = {}
                peerRatioDict['MPPClient'] = peersData['CompanyName']

                if peerRatios:
                    peerRatioData = json.loads(peerRatios["Content"])

                    for rKey in peerRatioData.keys():
                        peerRatioDict[rKey] = peerRatioData[rKey]

                    peerGroupRatios.append(peerRatioDict)
                    rawKpiValuesCopy.append({key: value for key, value in peerRatioDict.items()})

            peerGroupRatiosOutlier = self.normalize_measures(peerGroupRatios)
            normalizedGroupRatios = self.normalize_peer_measures(peerGroupRatiosOutlier)
            group_ratios_after_reversing = self.reverse_kpis_and_flags(normalizedGroupRatios)
            group_ratios_with_measures = self.calculate_measures_from_normalized_kpis(group_ratios_after_reversing)

            peer_comparison_results = self.prepare_json_for_api_worker(rawKpiValuesCopy, group_ratios_with_measures)

            return peer_comparison_results
        else:
            return {}
