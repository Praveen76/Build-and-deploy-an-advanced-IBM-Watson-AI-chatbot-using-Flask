from snp.capiq import Capiq
from datetime import datetime

class GetSnpData:
    def __init__(self):
        self.capObj = Capiq()

    def get_key_dev_leadership_ids(self, company_id):
      identifier = company_id
      MNEMONICS_KEY_DEV_ID = ['IQ_KEY_DEV_ID']
      properties = {'keyDevFilter': 'T16,T101,T102'}
      key_dev_ids = self.capObj.gdshe(identifier, MNEMONICS_KEY_DEV_ID, [], properties)[0]
      return key_dev_ids

    def get_leadership_changes(self, company_id):
      try:
        MNEMONICS_KEY_DEV_NEWS = ['IQ_KEY_DEV_DATE', 'IQ_KEY_DEV_HEADLINE']
        key_dev_id = self.get_key_dev_leadership_ids(company_id)
        leadership_changes = []
        for id in key_dev_id:
          news = self.capObj.gdsp(id, MNEMONICS_KEY_DEV_NEWS, [], [])
          news_date = datetime.strptime(news['IQ_KEY_DEV_DATE'], '%m/%d/%Y %H:%M:%S')
          news_date = datetime.strftime(news_date, "%b-%Y")
          news_headline = news['IQ_KEY_DEV_HEADLINE']
          leadership_changes.append(news_date + ': ' + news_headline)
        return leadership_changes
      except:
        msg = ["No leadership changes."]
        return msg

    def get_key_dev_aquisition_ids(self, company_id):
      identifier = company_id
      MNEMONICS_KEY_DEV_ID = ['IQ_KEY_DEV_ID']
      properties = {'keyDevFilter': 'T80,T81,T83'}
      key_dev_ids = self.capObj.gdshe(identifier, MNEMONICS_KEY_DEV_ID, [], properties)
      return key_dev_ids[0]

    def get_acquisitions(self, company_id):
      try:
        MNEMONICS_KEY_DEV_NEWS = ['IQ_KEY_DEV_DATE', 'IQ_KEY_DEV_HEADLINE']
        key_dev_id = self.get_key_dev_aquisition_ids(company_id)
        acquisitions = []
        for id in key_dev_id:
          news = self.capObj.gdsp(id, MNEMONICS_KEY_DEV_NEWS, [], [])
          news_date = datetime.strptime(news['IQ_KEY_DEV_DATE'], '%m/%d/%Y %H:%M:%S')
          news_date = datetime.strftime(news_date, "%b-%Y")
          news_headline = news['IQ_KEY_DEV_HEADLINE']
          acquisitions.append(news_date + ': ' + news_headline)
        return acquisitions
      except:
        msg = ["No acquisitions."]
        return msg

    def get_matched_company_names(self, company_name):
        MNEMONICS = ['IQ_COMPANY_ID_QUICK_MATCH', 'IQ_COMPANY_NAME_QUICK_MATCH']
        # Request
        # 1: (Gets the top 5 matched Company Names)
        # {"function": "GDSHE", "identifier": "AT&T", "mnemonic": "IQ_COMPANY_NAME_QUICK_MATCH",

        properties = {"startrank": "1", "endrank": "1"}
        self.capObj = Capiq()
        identifier = company_name
        key_dev_ids = self.capObj.gdshe(identifier, MNEMONICS, [], properties)
        return key_dev_ids

    def get_key_dev_product_ids(self, company_id):
      	identifier = company_id
      	MNEMONICS_KEY_DEV_ID = ['IQ_KEY_DEV_ID']
      	properties = {'keyDevFilter':'T41'}
      	key_dev_ids = self.capObj.gdshe(identifier, MNEMONICS_KEY_DEV_ID, [], properties)[0]
      	return key_dev_ids

    def get_key_products(self, company_id):
        try:
          MNEMONICS_KEY_DEV_NEWS = ['IQ_KEY_DEV_DATE', 'IQ_KEY_DEV_HEADLINE']
          key_dev_id = self.get_key_dev_product_ids(company_id)
          key_products = []
          for id in key_dev_id:
            news = self.capObj.gdsp(id, MNEMONICS_KEY_DEV_NEWS, [], [])
            news_date = datetime.strptime(news['IQ_KEY_DEV_DATE'], '%m/%d/%Y %H:%M:%S')
            news_date = datetime.strftime(news_date, "%b-%Y")
            news_headline = news['IQ_KEY_DEV_HEADLINE']
            key_products.append(news_date + ': ' + news_headline)
          return key_products
        except:
          msg = ["No key products and services."]
          return msg

    def get_key_dev_tech_adv_ids(self, company_id):
      	identifier = company_id
      	MNEMONICS_KEY_DEV_ID = ['IQ_KEY_DEV_ID']
      	properties = {'keyDevFilter':'T23'}
      	key_dev_ids = self.capObj.gdshe(identifier, MNEMONICS_KEY_DEV_ID, [], properties)[0]
      	return key_dev_ids

    def get_technological_advancements(self, company_id):
      try:
      	MNEMONICS_KEY_DEV_NEWS = ['IQ_KEY_DEV_DATE', 'IQ_KEY_DEV_HEADLINE']
      	key_dev_id = self.get_key_dev_tech_adv_ids(company_id)
      	client_announcements = []
      	for id in key_dev_id:
          news = self.capObj.gdsp(id, MNEMONICS_KEY_DEV_NEWS, [], [])
          news_date = datetime.strptime(news['IQ_KEY_DEV_DATE'], '%m/%d/%Y %H:%M:%S')
          news_date = datetime.strftime(news_date, "%b-%Y")
          news_headline = news['IQ_KEY_DEV_HEADLINE']
          client_announcements.append(news_date + ': ' + news_headline)
      	return client_announcements
      except:
        msg = ["No technological advancements."]
        return msg

    def get_company_financials(self, cap_iq_id, period_offset=0):
        """
        Gets {offset} * 3 months worth of historical financials relative to the current period
        :param cap_iq_id:
        :param offset: roll back in time by {offset} financial quarters
        :return:
        """
        MNEMONICS = [  # Financial Performance Indicators
            'IQ_FISCAL_Q', 'IQ_FISCAL_Y',
            'IQ_EMPLOYEES',
            'IQ_TOTAL_REV', 'IQ_TOTAL_REV_1YR_ANN_GROWTH',
            'IQ_EBIT', 'IQ_EBIT_1YR_ANN_GROWTH',
            'IQ_NI', 'IQ_NI_1YR_ANN_GROWTH',
            'IQ_TOTAL_ASSETS', 'IQ_TOTAL_ASSETS_1YR_ANN_GROWTH',
            'IQ_TOTAL_EQUITY', 'IQ_TOTAL_EQUITY_1YR_ANN_GROWTH',
            # Key KPIs
            'IQ_FIXED_ASSET_TURNS',
            'IQ_RETURN_ASSETS',
            'IQ_EBITA_MARGIN',
            'IQ_EBITDA_MARGIN',
            'IQ_GROSS_MARGIN',
            'IQ_NI_MARGIN',
            'IQ_RETURN_EQUITY',
            'IQ_EBIT_MARGIN',
            'IQ_CAPEX_1YR_ANN_GROWTH',
            'IQ_INV_1YR_ANN_GROWTH']

        properties = {"periodType": "IQ_LTM-{}".format(str(period_offset))}

        self.capObj = Capiq()
        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data

    def get_company_overview(self, cap_iq_id):
        MNEMONICS = ['IQ_SHORT_BUSINESS_DESCRIPTION', 'IQ_BUSINESS_DESCRIPTION']
        properties = {"periodType": "IQ_LTM"}

        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data

    def get_company_highlights(self, cap_iq_id):
        MNEMONICS = ['IQ_COMPANY_ADDRESS', 'IQ_COMPANY_NAME', 'IQ_COMPANY_NAME_LONG', 'IQ_COMPANY_PHONE',
                     'IQ_COMPANY_WEBSITE', 'IQ_EXCHANGE', 'IQ_COMPANY_MAIN_FAX',
                     'IQ_EQUITY_LIST']
        properties = {"periodType": "IQ_LTM"}

        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data

    def get_company_stock_trailing(self, cap_iq_id, months):

        MNEMONICS = ['IQ_ANNUALIZED_DIVIDEND']
        properties = {"startDate": "01/01/2018", "endDate": "12/31/2018"}

        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data

    def get_fiscal_year(self, cap_iq_id):
        MNEMONICS = ['IQ_FISCAL_Y']
        properties = {"periodType": "IQ_LTM"}

        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data

    def get_fiscal_quarter(self, cap_iq_id):
        MNEMONICS = ['IQ_FISCAL_Q']
        properties = {"periodType": "IQ_LTM"}

        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data


    def get_client_officers(self, cap_iq_id):
        #To be changed to appropriate mnemonic later
        MNEMONICS = ['IQ_BOARD_MEMBER','IQ_BOARD_MEMBER_ALL_OTHER_COMP'
                    ,'IQ_BOARD_MEMBER_ANNUAL_CASH_COMP','IQ_BOARD_MEMBER_AS_REPORTED_COMP','IQ_BOARD_MEMBER_AS_REPORTED_DIRECTOR_COMP',
                     'IQ_BOARD_MEMBER_ASSISTANT_EMAIL','IQ_BOARD_MEMBER_ASSISTANT_FAX','IQ_BOARD_MEMBER_ASSISTANT_NAME','IQ_BOARD_MEMBER_ASSISTANT_PHONE'
                     ,'IQ_BOARD_MEMBER_BACKGROUND','IQ_BOARD_MEMBER_BONUS','IQ_BOARD_MEMBER_CALCULATED_COMP','IQ_BOARD_MEMBER_CHANGE_PENSION','IQ_BOARD_MEMBER_DIRECT_FAX',
                     'IQ_BOARD_MEMBER_DIRECT_PHONE','IQ_BOARD_MEMBER_DIRECTOR_BONUS','IQ_BOARD_MEMBER_DIRECTOR_CHANGE_PENSION','IQ_BOARD_MEMBER_DIRECTOR_FEE',
                     'IQ_BOARD_MEMBER_DIRECTOR_NON_EQUITY_COMP','IQ_BOARD_MEMBER_DIRECTOR_OPTION_AWARDS','IQ_BOARD_MEMBER_DIRECTOR_OTHER','IQ_BOARD_MEMBER_DIRECTOR_STOCK_AWARDS',
                     'IQ_BOARD_MEMBER_DIRECTOR_STOCK_GRANTS','IQ_BOARD_MEMBER_DIRECTOR_STOCK_OPTIONS','IQ_BOARD_MEMBER_EMAIL','IQ_BOARD_MEMBER_EQUITY_INCENTIVE',
                     'IQ_BOARD_MEMBER_EST_PAYMENTS_CHANGE_CONTROL','IQ_BOARD_MEMBER_EST_PAYMENTS_TERMINATION','IQ_BOARD_MEMBER_EXERCISABLE_OPTIONS','IQ_BOARD_MEMBER_EXERCISABLE_VALUES',
                     'IQ_BOARD_MEMBER_EXERCISED_OPTIONS','IQ_BOARD_MEMBER_EXERCISED_VALUES','IQ_BOARD_MEMBER_ID','IQ_BOARD_MEMBER_LT_INCENTIVE',
                     'IQ_BOARD_MEMBER_MAIN_FAX','IQ_BOARD_MEMBER_MAIN_PHONE','IQ_BOARD_MEMBER_MARKET_VALUE_SHARES_NOT_VESTED',
                     'IQ_BOARD_MEMBER_NON_EQUITY_INCENTIVE','IQ_BOARD_MEMBER_NUM_SHARED_NOT_VESTED','IQ_BOARD_MEMBER_NUM_SHARES_ACQUIRED',
                     'IQ_BOARD_MEMBER_OFFICE_ADDRESS','IQ_BOARD_MEMBER_OPTION_AWARDS','IQ_BOARD_MEMBER_OPTION_MARKET_PRICE','IQ_BOARD_MEMBER_OPTION_PRICE',
                     'IQ_BOARD_MEMBER_OTHER_ANNUAL_COMP','IQ_BOARD_MEMBER_OTHER_COMP','IQ_BOARD_MEMBER_RESTRICTED_STOCK_COMP','IQ_BOARD_MEMBER_SALARY',
                     'IQ_BOARD_MEMBER_ST_COMP','IQ_BOARD_MEMBER_TITLE','IQ_BOARD_MEMBER_TOTAL_NUM_STOCK_AWARDS','IQ_BOARD_MEMBER_TOTAL_OPTIONS',
                     'IQ_BOARD_MEMBER_TOTAL_STOCK_VALUE','IQ_BOARD_MEMBER_TOTAL_VALUE_OPTIONS','IQ_BOARD_MEMBER_UNCLASSIFIED_OPTIONS','IQ_BOARD_MEMBER_UNCLASSIFIED_OPTIONS_VALUE','IQ_BOARD_MEMBER_UNEARNED_STOCK_VALUE','IQ_BOARD_MEMBER_UNEXERCISABLE_OPTIONS','IQ_BOARD_MEMBER_UNEXERCISABLE_VALUES','IQ_BOARD_MEMBER_UNEXERCISED_UNEARNED_OPTIONS','IQ_BOARD_MEMBER_UNEXERCISED_UNEARNED_OPTIONS_VALUE','IQ_BOARD_MEMBER_VALUE_VESTING','IQ_INDIVIDUAL','IQ_INDIVIDUAL_ACTIVE_BOARD_MEMBERSHIPS','IQ_INDIVIDUAL_ACTIVE_PRO_AFFILIATIONS','IQ_INDIVIDUAL_AGE','IQ_INDIVIDUAL_ALL_OTHER_COMP','IQ_INDIVIDUAL_ANNUAL_CASH_COMP','IQ_INDIVIDUAL_AS_REPORTED_COMP','IQ_INDIVIDUAL_AS_REPORTED_DIRECTOR_COMP','IQ_INDIVIDUAL_ASSISTANT_NAME','IQ_INDIVIDUAL_BACKGROUND','IQ_INDIVIDUAL_BONUS','IQ_INDIVIDUAL_CALCULATED_COMP','IQ_INDIVIDUAL_CHANGE_PENSION','IQ_INDIVIDUAL_DIRECTOR_BONUS','IQ_INDIVIDUAL_DIRECTOR_CHANGE_PENSION','IQ_INDIVIDUAL_DIRECTOR_FEE','IQ_INDIVIDUAL_DIRECTOR_NON_EQUITY_COMP','IQ_INDIVIDUAL_DIRECTOR_OPTION_AWARDS','IQ_INDIVIDUAL_DIRECTOR_OTHER','IQ_INDIVIDUAL_DIRECTOR_STOCK_AWARDS','IQ_INDIVIDUAL_DIRECTOR_STOCK_GRANTS','IQ_INDIVIDUAL_DIRECTOR_STOCK_OPTIONS','IQ_INDIVIDUAL_EDUCATION','IQ_INDIVIDUAL_EQUITY_INCENTIVE','IQ_INDIVIDUAL_EST_PAYMENTS_CHANGE_CONTROL','IQ_INDIVIDUAL_EST_PAYMENTS_TERMINATION','IQ_INDIVIDUAL_EXERCISABLE_OPTIONS','IQ_INDIVIDUAL_EXERCISABLE_VALUES','IQ_INDIVIDUAL_EXERCISED_OPTIONS','IQ_INDIVIDUAL_EXERCISED_VALUES','IQ_INDIVIDUAL_HOME_ADDRESS','IQ_INDIVIDUAL_JOB_FUNCTIONS','IQ_INDIVIDUAL_LT_INCENTIVE','IQ_INDIVIDUAL_MARKET_VALUE_SHARES_NOT_VESTED','IQ_INDIVIDUAL_NICKNAME','IQ_INDIVIDUAL_NON_EQUITY_INCENTIVE','IQ_INDIVIDUAL_NOTES','IQ_INDIVIDUAL_NUM_SHARED_NOT_VESTED','IQ_INDIVIDUAL_NUM_SHARES_ACQUIRED','IQ_INDIVIDUAL_OFFICE_ADDRESS','IQ_INDIVIDUAL_OPTION_AWARDS','IQ_INDIVIDUAL_OPTION_MARKET_PRICE','IQ_INDIVIDUAL_OPTION_PRICE','IQ_INDIVIDUAL_OTHER_ANNUAL_COMP','IQ_INDIVIDUAL_OTHER_COMP','IQ_INDIVIDUAL_PRIOR_BOARD_MEMBERSHIPS','IQ_INDIVIDUAL_PRIOR_PRO_AFFILIATIONS','IQ_INDIVIDUAL_RESTRICTED_STOCK_COMP','IQ_INDIVIDUAL_SALARY','IQ_INDIVIDUAL_SPECIALTY','IQ_INDIVIDUAL_ST_COMP','IQ_INDIVIDUAL_TITLE','IQ_INDIVIDUAL_TOTAL_NUM_STOCK_AWARDS','IQ_INDIVIDUAL_TOTAL_OPTIONS','IQ_INDIVIDUAL_TOTAL_STOCK_VALUE','IQ_INDIVIDUAL_TOTAL_VALUE_OPTIONS','IQ_INDIVIDUAL_UNCLASSIFIED_OPTIONS','IQ_INDIVIDUAL_UNCLASSIFIED_OPTIONS_VALUE','IQ_INDIVIDUAL_UNEARNED_STOCK_VALUE','IQ_INDIVIDUAL_UNEXERCISABLE_OPTIONS','IQ_INDIVIDUAL_UNEXERCISABLE_VALUES','IQ_INDIVIDUAL_UNEXERCISED_UNEARNED_OPTIONS','IQ_INDIVIDUAL_UNEXERCISED_UNEARNED_OPTIONS_VALUE','IQ_INDIVIDUAL_VALUE_VESTING','IQ_PROFESSIONAL','IQ_PROFESSIONAL_ALL_OTHER_COMP','IQ_PROFESSIONAL_ANNUAL_CASH_COMP','IQ_PROFESSIONAL_AS_REPORTED_COMP','IQ_PROFESSIONAL_AS_REPORTED_DIRECTOR_COMP','IQ_PROFESSIONAL_ASSISTANT_EMAIL','IQ_PROFESSIONAL_ASSISTANT_FAX','IQ_PROFESSIONAL_ASSISTANT_NAME','IQ_PROFESSIONAL_ASSISTANT_PHONE','IQ_PROFESSIONAL_BACKGROUND','IQ_PROFESSIONAL_BONUS','IQ_PROFESSIONAL_CALCULATED_COMP','IQ_PROFESSIONAL_CHANGE_PENSION','IQ_PROFESSIONAL_DIRECT_FAX','IQ_PROFESSIONAL_DIRECT_PHONE','IQ_PROFESSIONAL_DIRECTOR_BONUS','IQ_PROFESSIONAL_DIRECTOR_CHANGE_PENSION','IQ_PROFESSIONAL_DIRECTOR_FEE','IQ_PROFESSIONAL_DIRECTOR_NON_EQUITY_COMP','IQ_PROFESSIONAL_DIRECTOR_OPTION_AWARDS','IQ_PROFESSIONAL_DIRECTOR_OTHER','IQ_PROFESSIONAL_DIRECTOR_STOCK_AWARDS','IQ_PROFESSIONAL_DIRECTOR_STOCK_GRANTS','IQ_PROFESSIONAL_DIRECTOR_STOCK_OPTIONS','IQ_PROFESSIONAL_EMAIL','IQ_PROFESSIONAL_EQUITY_INCENTIVE','IQ_PROFESSIONAL_EST_PAYMENTS_CHANGE_CONTROL','IQ_PROFESSIONAL_EST_PAYMENTS_TERMINATION','IQ_PROFESSIONAL_EXERCISABLE_OPTIONS','IQ_PROFESSIONAL_EXERCISABLE_VALUES','IQ_PROFESSIONAL_EXERCISED_OPTIONS','IQ_PROFESSIONAL_EXERCISED_VALUES','IQ_PROFESSIONAL_ID','IQ_PROFESSIONAL_LT_INCENTIVE','IQ_PROFESSIONAL_MAIN_FAX','IQ_PROFESSIONAL_MAIN_PHONE','IQ_PROFESSIONAL_MARKET_VALUE_SHARES_NOT_VESTED','IQ_PROFESSIONAL_NON_EQUITY_INCENTIVE','IQ_PROFESSIONAL_NUM_SHARED_NOT_VESTED','IQ_PROFESSIONAL_NUM_SHARES_ACQUIRED','IQ_PROFESSIONAL_OFFICE_ADDRESS','IQ_PROFESSIONAL_OPTION_AWARDS','IQ_PROFESSIONAL_OPTION_MARKET_PRICE','IQ_PROFESSIONAL_OPTION_PRICE','IQ_PROFESSIONAL_OTHER_ANNUAL_COMP','IQ_PROFESSIONAL_OTHER_COMP','IQ_PROFESSIONAL_RESTRICTED_STOCK_COMP','IQ_PROFESSIONAL_SALARY','IQ_PROFESSIONAL_ST_COMP','IQ_PROFESSIONAL_TITLE','IQ_PROFESSIONAL_TOTAL_NUM_STOCK_AWARDS','IQ_PROFESSIONAL_TOTAL_OPTIONS','IQ_PROFESSIONAL_TOTAL_STOCK_VALUE','IQ_PROFESSIONAL_TOTAL_VALUE_OPTIONS','IQ_PROFESSIONAL_UNCLASSIFIED_OPTIONS','IQ_PROFESSIONAL_UNCLASSIFIED_OPTIONS_VALUE','IQ_PROFESSIONAL_UNEARNED_STOCK_VALUE','IQ_PROFESSIONAL_UNEXERCISABLE_OPTIONS','IQ_PROFESSIONAL_UNEXERCISABLE_VALUES','IQ_PROFESSIONAL_UNEXERCISED_UNEARNED_OPTIONS','IQ_PROFESSIONAL_UNEXERCISED_UNEARNED_OPTIONS_VALUE','IQ_PROFESSIONAL_VALUE_VESTING']
        # To be changed to appropriate properties later
        properties = {"startDate": "01/01/2018", "endDate": "12/31/2018"}

        identifier = cap_iq_id
        data = self.capObj.gdshe(identifier, MNEMONICS, [], properties)
        
        return data

    def get_NAICS_code(self, cap_iq_id):
        MNEMONICS = ['IQ_BUS_SEG_NAIC']
        properties = {"periodType": "IQ_LTM"}

        identifier = cap_iq_id
        data = self.capObj.gdsp(identifier, MNEMONICS, [], properties)
        return data
