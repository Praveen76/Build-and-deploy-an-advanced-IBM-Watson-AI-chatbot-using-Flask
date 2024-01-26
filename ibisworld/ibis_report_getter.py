import requests
import json
import base64
from ibisworld.authenticator import OauthAuthenticator
from cbda.cbda_client import CbdaClient
from ibisworld.ibis_mapping_retrieval import IbisMappingRetrieval
from datetime import datetime


KEY_STATISTICS_TABLE_CELLS = 'KeyStatisticsTableCells'
KEY_STATISTICS_IMAGE = 'KeyStatisticsSnapshotImage'
KEY_STATISTICS_TABLE = 'KeyStatisticsTable'
KEY_SUCCESS_FACTORS = 'KeySuccessFactors'
SECTOR_INDUSTRY_COSTS_CHART = 'SectorIndustryCostsChart'
PRODUCT_AND_SERVICES_SEGMENTATION_CHART_DATA = 'ProductsAndServicesSegmentationChartData'
PRODUCT_AND_SERVICES_SEGMENTATION_CHART = 'ProductsAndServicesSegmentationChart'
MAJOR_PLAYERS_CHART = 'MajorPlayersChart'
REVENUE_CHART = 'RevenueChart'
KEY_ECONOMIC_DRIVERS_TABLE = 'KeyEconomicDriversTable'
KEY_EXTERNAL_DRIVERS_CHART = 'KeyExternalDriversChart'
THREAT_ANALYSIS = 'ThreatAnalysis'
OPPORTUNITY_ANALYSIS = 'OpportunityAnalysis'
EXECUTIVE_SUMMARY = 'ExecutiveSummaryAnalysis'
INDUSTRY_HEADLINE = 'Caption'
INDUSTRY_DEFINITION = 'IndustryDefinition'
SUPPLY_INDUSTRIES_TABLE = 'SupplyIndustriesTable'
SIMILAR_INDUSTRIES_TABLE = 'SimilarIndustriesTable'
DEMAND_INDUSTRIES_TABLE = 'DemandIndustriesTable'
INTERNATIONAL_CONCORDANCE = 'InternationalConcordance'
ANALYSIS_PLAIN_TEXT = 'AnalysisPlainText'
CHART = 'Base64'
CONFIG_PATH = ""
BASE_URI = 'https://developer.ibisworld.com/'

class IBISWorldReportExtractor(object):
    def __init__(self, entity_id, config_path=None):
        auth = OauthAuthenticator(config_path)
        self.access_token = auth.get_access_token()
        client = CbdaClient()
        response = client.get_client_industry(entity_id)
        self.LOB = response['LOB']
        self.entity_id = entity_id
        self.mapping = IbisMappingRetrieval()
        self.naics_code = self.mapping.get_naics_code(LOB_Name=response['LOB'], Sector=response['Sector_ID'],
                                      Segment=response['Segment_ID'])
        self.industry_report = self.get_industry_report()
        self.iexpert_report = self.get_iexpert_report()

    def get_report(self, api_endpoint, request_body):
        headers = {"Authorization":"Bearer {0}".format(self.access_token),
            "Content-Type":"application/json",}
        try:
            response = requests.post(url=api_endpoint,headers=headers,data=json.dumps(request_body))
            return response.json()
        except Exception as e:
            raise e

    def get_iexpert_report(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON"\
                .format(BASE_URI, 'FullReport')
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            return response
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_industry_report(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/IExpert/{1}/JSON"\
                .format(BASE_URI, 'FullReport')
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            return response
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_industry_threats(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/IExpert/{1}/JSON"\
                .format(BASE_URI, THREAT_ANALYSIS)
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            threats = response.get(ANALYSIS_PLAIN_TEXT)
            return threats
        except Exception:
            return {"error": "Exception fetching data from IbisWorld!"}

    def get_industry_opportutnities(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/IExpert/{1}/JSON"\
                .format(BASE_URI,OPPORTUNITY_ANALYSIS)
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            opportunities = response.get(ANALYSIS_PLAIN_TEXT)
            return opportunities
        except Exception:
            return {"Error": "Exception fetching data from IbisWorld!"}

    def get_key_success_factors(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON"\
                .format(BASE_URI,KEY_SUCCESS_FACTORS)
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            return response
        except Exception as e:
            return "Exception fetching data from IbisWorld!"

    def get_product_and_segmentation_data(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON"\
                .format(BASE_URI,PRODUCT_AND_SERVICES_SEGMENTATION_CHART_DATA)
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            data = {"product_and_segmentation_data":response}
            return data
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_key_external_drivers(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON"\
                .format(BASE_URI,KEY_ECONOMIC_DRIVERS_TABLE)
            request_body = {"Code":industry_code, "Country":country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            data = []
            for item in response:
                data.append(item.get('Driver'))

            drivers = {"key_external_drivers":data}
            return drivers
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_executive_summary_analysis(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON" \
                .format(BASE_URI, EXECUTIVE_SUMMARY)
            request_body = {"Code": industry_code, "Country": country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            executive_summary = response.get(ANALYSIS_PLAIN_TEXT)
            #executive_summary = str(executive_summary).split('\n')[0]
            return executive_summary
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_industry_headline(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON" \
                .format(BASE_URI, INDUSTRY_HEADLINE)
            request_body = {"Code": industry_code, "Country": country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            industry_headline = response.get(ANALYSIS_PLAIN_TEXT)
            return industry_headline
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_industry_definition(self, country_code=1, report_id=None):
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON" \
                .format(BASE_URI, INDUSTRY_DEFINITION)
            request_body = {"Code": industry_code, "Country": country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            industry_definition = response.get(ANALYSIS_PLAIN_TEXT)
            return industry_definition
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_supply_chain_reports(self, report_type, country_code=1, report_id=None):
        data = []
        try:
            industry_code = self.naics_code
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON" \
                .format(BASE_URI, report_type)
            request_body = {"Code": industry_code, "Country": country_code}
            if report_id:
                request_body['ReportID'] = report_id
            response = self.get_report(api_endpoint, request_body)
            for item in response:
                data.append(item.get('Industry'))
            return data
        except Exception:
            return data

    def get_supply_industries(self, country_code=1, report_id=None):
        try:
            industries = self.get_supply_chain_reports(SUPPLY_INDUSTRIES_TABLE)
            supply_industries = {"supply_industries": industries}
            return supply_industries
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_related_industries(self, country_code=1, report_id=None):
        try:
            industries = self.get_supply_chain_reports(SIMILAR_INDUSTRIES_TABLE)
            related_industries = {"related_industries": industries}
            return related_industries
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_demand_industries(self, country_code=1, report_id=None):
        try:
            industries = self.get_supply_chain_reports(DEMAND_INDUSTRIES_TABLE)
            demand_industries = {"demand_industries": industries}
            return demand_industries
        except Exception:
            return "Exception fetching data from IbisWorld!"\

    def get_related_international_industries(self, country_code=1, report_id=None):
        try:
            industries = self.get_supply_chain_reports(INTERNATIONAL_CONCORDANCE)
            related_international_industries = {"related_international_industries": industries}
            return related_international_industries
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_supply_chain(self, country_code=1, report_id=None):
        try:
            supply_industries = self.get_supply_industries()
            demand_industries = self.get_demand_industries()
            related_industries = self.get_related_industries()
            key_external_drivers = self.get_key_external_drivers()
            related_international_industries = self.get_related_international_industries()
            supply_chain = {**supply_industries, **related_industries, **related_international_industries,
                            **demand_industries, **key_external_drivers}
            return supply_chain
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_operating_conditions(self, country_code=1, report_id=None):
        try:
            capital_intensity_level = self.industry_report.get('CapitalIntensityLevel').get('Value')
            technology_change_level = self.industry_report.get('TechnologyChangeLevel').get('Value')
            revenue_volatility_level = self.industry_report.get('RevenueVolatilityLevel')
            regulation_policy_level = self.industry_report.get('RegulationLevel').get('Value')
            industry_assistance = self.industry_report.get('IndustryAssistanceLevel').get('Value')

            operating_conditions = {
                "capital_intensity_change_level":capital_intensity_level,
                "technology_change_level":technology_change_level,
                "revenue_volaility_level":revenue_volatility_level,
                "regulation_policy_level":regulation_policy_level,
                "industry_assistance_level":industry_assistance
                }

            return operating_conditions
        except Exception:
            return {"error": "Exception fetching data from IbisWorld!"}

    def get_industry_glance(self, country_code=1, report_id=None):
        try:
            product_and_segmentation_data = self.get_product_and_segmentation_data(country_code, report_id)
            industry_glance = {}
            industry_glance[KEY_STATISTICS_IMAGE] = self.industry_report.get(KEY_STATISTICS_IMAGE).get(CHART)
            industry_glance[KEY_STATISTICS_TABLE] = self.industry_report.get(KEY_STATISTICS_TABLE)
            industry_glance[MAJOR_PLAYERS_CHART] = self.iexpert_report.get(MAJOR_PLAYERS_CHART).get(CHART)
            industry_glance[SECTOR_INDUSTRY_COSTS_CHART] = self.industry_report.get(SECTOR_INDUSTRY_COSTS_CHART).get(CHART)
            industry_glance[REVENUE_CHART] = self.iexpert_report.get(REVENUE_CHART).get(CHART)
            prods = product_and_segmentation_data
            key_drivers = self.get_key_external_drivers()
            industry_insights = {**industry_glance, **key_drivers, **prods}
            return industry_insights
        except Exception:
            return "Error retrieving data from IBISWorld"

    def get_industry_outlook(self, country_code=1, report_id=None):
        try:
            outlook = []
            current_year = datetime.now().year
            cells = self.industry_report.get(KEY_STATISTICS_TABLE).get(KEY_STATISTICS_TABLE_CELLS)

            count = 0;
            for data in cells:
                if data['ColumnID'] != 1:
                    break
                year = data['Year']
                revenue = data['CurrentValue']
                if year == current_year:
                    current_revenue = revenue
                elif year > current_year:
                    percentage = '{0:.2f}%'.format(((revenue-current_revenue) / current_revenue * 100))
                    outlook.append({'Year': str(year), 'Revenue': str(revenue), 'Growth': str(percentage)})
                    current_revenue = revenue

            return outlook
        except Exception as e:
            print("Error retrieving data from IBISWorld- {}".format(e))
            return []

    def get_report_list(self, country_code=1):
        try:
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON"\
                .format(BASE_URI,'ReportList')
            request_body = {"Country":country_code}
            report_list = self.get_report(api_endpoint, request_body)
            return report_list
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def get_updated_reports(self, updated_from, updated_to=None, country_code=1):
        try:
            api_endpoint = "{0}/Developer/api/v2.1/external/Industry/{1}/JSON"\
                .format(BASE_URI,'UpdatedReports')
            request_body = {"Country":country_code, "UpdatedFrom":updated_from}
            if updated_to:
                request_body["UpdatedTo"] = updated_to
            updated_report_list = self.get_report(api_endpoint, request_body)
            return updated_report_list
        except Exception:
            return "Exception fetching data from IbisWorld!"

    def reset_naics(self, LOB, sector, segment):
        try:
            self.naics_code = self.mapping.get_naics_code(LOB_Name=LOB, Sector=sector,
                                                     Segment=segment)
            return self.naics_code
        except Exception:
            return None
