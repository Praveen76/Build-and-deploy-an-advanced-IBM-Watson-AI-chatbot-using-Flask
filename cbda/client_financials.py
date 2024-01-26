import datetime
import numpy as np
from cbda.cbda_client import CbdaClient
from snp.get_snp_data import GetSnpData

class ClientFinancials:
    def __init__(self):
        self.client = CbdaClient()

    def get_client_financials(self, entity_id):
        """
        Current year financials with growth over prior year
        """

        client_data = self.client.get_client(entity_id)

        if client_data['CapitalIQID']:
            snp = GetSnpData()

            # Financials for reported TTM as well as TTM for the year prior
            current_financials = snp.get_company_financials(client_data['CapitalIQID'])
            previous_financials = snp.get_company_financials(client_data['CapitalIQID'], "4")

            # Growth over prior year calculations
            employees_growth = int(float(current_financials['IQ_EMPLOYEES'])) - \
                               int(float(previous_financials['IQ_EMPLOYEES']))

            # IQ_TOTAL_REV_1YR_ANN_GROWTH
            rev_growth = float(current_financials['IQ_TOTAL_REV']) - \
                         float(previous_financials['IQ_TOTAL_REV'])

            #  IQ_EBIT_1YR_ANN_GROWTH
            operating_income_growth = float(current_financials['IQ_EBIT']) - \
                                      float(previous_financials['IQ_EBIT'])

            # IQ_NI_1YR_ANN_GROWTH
            net_income_growth = float(current_financials['IQ_NI']) - \
                                float(previous_financials['IQ_NI'])

            # IQ_TOTAL_ASSETS_1YR_ANN_GROWTH
            total_assets_growth = float(current_financials['IQ_TOTAL_ASSETS']) - \
                                  float(previous_financials['IQ_TOTAL_ASSETS'])

            # IQ_COMMON_EQUITY_1YR_ANN_GROWTH --> this mnemonic is for industry specific only
            total_equity_growth = float(current_financials['IQ_TOTAL_EQUITY']) - \
                                  float(previous_financials['IQ_TOTAL_EQUITY'])

            # These are not directly available on S&P and need to be calculated
            fa_utilization_growth = float(current_financials['IQ_FIXED_ASSET_TURNS']) - \
                                    float(previous_financials['IQ_FIXED_ASSET_TURNS'])

            roa_growth = float(current_financials['IQ_RETURN_ASSETS']) - \
                         float(previous_financials['IQ_RETURN_ASSETS'])

            oe_margin_growth = float(current_financials['IQ_EBITA_MARGIN']) - \
                               float(previous_financials['IQ_EBITA_MARGIN'])

            ebitda_margin_growth = float(current_financials['IQ_EBITDA_MARGIN']) - \
                                   float(previous_financials['IQ_EBITDA_MARGIN'])

            gp_margin_growth = float(current_financials['IQ_GROSS_MARGIN']) - \
                               float(previous_financials['IQ_GROSS_MARGIN'])

            ni_margin_growth = float(current_financials['IQ_NI_MARGIN']) - \
                               float(previous_financials['IQ_NI_MARGIN'])

            roe_growth = float(current_financials['IQ_RETURN_EQUITY']) - \
                         float(previous_financials['IQ_RETURN_EQUITY'])

            operating_margin_growth = float(current_financials['IQ_EBIT_MARGIN']) - \
                                      float(previous_financials['IQ_EBIT_MARGIN'])

            # @TODO implement number formatting via locale locale.setlocale(locale.LC_ALL, 'EN_US.UTF8')
            data = [{
                'financial_performance': [
                    {'label': 'TOTAL_EMPLOYEES',
                     'value': "{:,}".format(int(float(current_financials['IQ_EMPLOYEES']))),
                     'growth_value': "{:,}".format(employees_growth),
                     'growth_percentage': abs(
                         self.percentage(employees_growth, float(previous_financials['IQ_EMPLOYEES']))),
                     'growth_sign': self.pct_change_sign(employees_growth)},

                    {'label': 'TOTAL_REVENUE',
                     'value': "{:,}".format(round(float(current_financials['IQ_TOTAL_REV']), 2)),
                     'growth_value': "{:,}".format(rev_growth),
                     'growth_percentage': "" if np.isnan(round(float(current_financials['IQ_TOTAL_REV_1YR_ANN_GROWTH']), 2)) else
                     round(float(current_financials['IQ_TOTAL_REV_1YR_ANN_GROWTH']), 2),
                     'growth_sign': self.pct_change_sign(rev_growth)},

                    {'label': 'OPERATING_INCOME',
                     'value': "{:,}".format(round(float(current_financials['IQ_EBIT']), 2)),
                     'growth_value': "{:,}".format(round(operating_income_growth, 2)),
                     'growth_percentage': "" if np.isnan(round(float(current_financials['IQ_EBIT_1YR_ANN_GROWTH']), 2)) else
                     round(float(current_financials['IQ_EBIT_1YR_ANN_GROWTH']), 2),
                     'growth_sign': self.pct_change_sign(operating_income_growth)},

                    {'label': 'NET_INCOME',
                     'value': "{:,}".format(round(float(current_financials['IQ_NI']), 2)),
                     'growth_value': "{:,}".format(round(net_income_growth, 2)),
                     'growth_percentage': "" if np.isnan(round(float(current_financials['IQ_NI_1YR_ANN_GROWTH']), 2)) else
                     round(float(current_financials['IQ_NI_1YR_ANN_GROWTH']), 2),
                     'growth_sign': self.pct_change_sign(net_income_growth)},

                    {'label': 'TOTAL_ASSETS',
                     'value': "{:,}".format(round(float(current_financials['IQ_TOTAL_ASSETS']), 2)),
                     'growth_value': "{:,}".format(round(total_assets_growth, 2)),
                     'growth_percentage': "" if np.isnan(round(float(current_financials['IQ_TOTAL_ASSETS_1YR_ANN_GROWTH']), 2)) else
                     round(float(current_financials['IQ_TOTAL_ASSETS_1YR_ANN_GROWTH']), 2),
                     'growth_sign': self.pct_change_sign(total_assets_growth)},

                    {'label': 'TOTAL_EQUITY',
                     'value': "{:,}".format(round(float(current_financials['IQ_TOTAL_EQUITY']), 2)),
                     'growth_value': "{:,}".format(round(total_equity_growth, 2)),
                     'growth_percentage': abs(
                         self.percentage(total_equity_growth, float(previous_financials['IQ_TOTAL_EQUITY']))),
                     'growth_sign': self.pct_change_sign(total_equity_growth)}
                ],
                'key_kpi': [
                    {'label': 'FIXED_ASSET_UTILIZATION',
                     'value': str(round(float(current_financials['IQ_FIXED_ASSET_TURNS']), 2)) + 'x',
                     'growth_value': round(fa_utilization_growth, 2),
                     'growth_percentage': abs(
                         self.percentage(fa_utilization_growth, previous_financials['IQ_FIXED_ASSET_TURNS'])),
                     'growth_sign': self.pct_change_sign(fa_utilization_growth)},

                    {'label': 'GMROI',
                     'value': "",
                     'growth_value': "",
                     'growth_percentage': "",
                     'growth_sign': ""},

                    {'label': 'RETURN_ON_ASSETS',
                     'value': round(float(current_financials['IQ_RETURN_ASSETS']), 2),
                     'growth_value': round(roa_growth, 2),
                     'growth_percentage': abs(self.percentage(roa_growth, previous_financials['IQ_RETURN_ASSETS'])),
                     'growth_sign': self.pct_change_sign(roa_growth)},

                    {'label': 'SG_n_A_PER_REV',
                     'value': "",
                     'growth_value': "",
                     'growth_percentage': "",
                     'growth_sign': ""},

                    {'label': 'OPERATING_EXPENSE_MARGIN',
                     'value': round(float(current_financials['IQ_EBITA_MARGIN']), 2),
                     'growth_value': round(oe_margin_growth, 2),
                     'growth_percentage': abs(
                         self.percentage(oe_margin_growth, previous_financials['IQ_EBITA_MARGIN'])),
                     'growth_sign': self.pct_change_sign(oe_margin_growth)},

                    {'label': 'EXPENSES_TO_ASSETS',
                     'value': "",
                     'growth_value': "",
                     'growth_percentage': "",
                     'growth_sign': ""},

                    {'label': 'EBITDA_MARGIN',
                     'value': round(float(current_financials['IQ_EBITDA_MARGIN']), 2),
                     'growth_value': round(ebitda_margin_growth, 2),
                     'growth_percentage': abs(
                         self.percentage(ebitda_margin_growth, previous_financials['IQ_EBITDA_MARGIN'])),
                     'growth_sign': self.pct_change_sign(ebitda_margin_growth)},

                    {'label': 'GROSS_PROFIT_MARGIN',
                     'value': round(float(current_financials['IQ_GROSS_MARGIN']), 2),
                     'growth_value': round(gp_margin_growth, 2),
                     'growth_percentage': abs(
                         self.percentage(gp_margin_growth, previous_financials['IQ_GROSS_MARGIN'])),
                     'growth_sign': self.pct_change_sign(gp_margin_growth)},

                    {'label': 'NET_INCOME_MARGIN',
                     'value': round(float(current_financials['IQ_NI_MARGIN']), 2),
                     'growth_value': round(ni_margin_growth, 2),
                     'growth_percentage': abs(
                         self.percentage(ni_margin_growth, previous_financials['IQ_NI_MARGIN'])),
                     'growth_sign': self.pct_change_sign(ni_margin_growth)},

                    {'label': 'RETURN_ON_EQUITY',
                     'value': round(float(current_financials['IQ_RETURN_EQUITY']), 2),
                     'growth_value': round(roe_growth, 2),
                     'growth_percentage': abs(self.percentage(roe_growth, previous_financials['IQ_RETURN_EQUITY'])),
                     'growth_sign': self.pct_change_sign(roe_growth)},

                    {'label': 'OPERATING_MARGIN',
                     'value': round(float(current_financials['IQ_EBIT_MARGIN']), 2),
                     'growth_value': round(operating_margin_growth, 2),
                     'growth_percentage': abs(
                         self.percentage(operating_margin_growth, previous_financials['IQ_EBIT_MARGIN'])),
                     'growth_sign': self.pct_change_sign(operating_margin_growth)},

                    {'label': 'REV_PER_EMPLOYEE',
                     'value': "",
                     'growth_value': "",
                     'growth_percentage': "",
                     'growth_sign': ""},

                    {'label': 'CAPEX_GROWTH_1YR',
                     'value': round(float(current_financials['IQ_CAPEX_1YR_ANN_GROWTH']), 2),
                     'growth_value': "",
                     'growth_percentage': "",
                     'growth_sign': ""}
                ],
                'FISCAL_YEAR': snp.get_fiscal_year(client_data['CapitalIQID']),
                'QUARTER': snp.get_fiscal_quarter(client_data['CapitalIQID'])
            }]
            return data

    def percentage(self, part, whole):
        return round(100 * float(part) / float(whole), 2)

    def pct_change_sign(self, param):
        return "Up" if param > 0 else ("Down" if param < 0 else '')
