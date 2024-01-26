import requests

from datetime import datetime, timedelta

class GetKogneticsData:
    def __init__(self):
        self.key = '2685683d26d6eb337e53316f9a0e5c6b'  ## As provided by Kognetics Team
        self.api_url_common = 'https://api.kognetics.com/v1.0/company/'

    def get_business_signals(self, company_permalink, api_topic, num_of_days = 365):
        api_name = 'BusinessSignals'
        
        ## Dates by difference in days from today's date in function call can be changed to reflect any period for which signals' data is required
            ## Used year long (365 days) default time period currently for all company signals
            
        date_today = datetime.now()
        end_dt = date_today.strftime("%Y-%m-%d")
        date_days_before = date_today - timedelta(days = num_of_days)
        start_dt = date_days_before.strftime("%Y-%m-%d")
       
        ## Creating the API Url to be targeted as per requirement
        api_url_parameters = '{0}?permalink={1}&startDate={2}&endDate={3}&businessSignal={4}&key={5}'.format(
                api_name, company_permalink, start_dt, end_dt, api_topic, self.key)
        req_url = self.api_url_common + api_url_parameters
        result = requests.get(req_url)
        output = result.json()

        ## Processing the result
        if result and result.status_code == 200:
          try:
              info = output['data']['rows']
          except KeyError:
              info = [{'message': output['message']}]
        else:
            err = "Error status: " + str(result.status_code)
            info = [{'error': err}]
        return info

    def get_company_news(self, company_permalink, num_of_days = 365):
        api_name = 'News'
        
        ## Dates can be changed to reflect any period for which news is required - default is 365 days

        date_today = datetime.now()
        end_dt = date_today.strftime("%Y-%m-%d")
        date_days_before = date_today - timedelta(days = num_of_days)
        start_dt = date_days_before.strftime("%Y-%m-%d")

        ## Creating the API Url to be targeted as per requirement
        api_url_parameters = '{0}?permalink={1}&startDate={2}&endDate={3}&key={4}'.format(
                api_name, company_permalink, start_dt, end_dt, self.key)
        req_url = self.api_url_common + api_url_parameters
        result = requests.get(req_url)
        output = result.json()

        ## Processing the result
        if result and result.status_code == 200:
          try:
              news = output['data']['rows']
          except KeyError:
              news = [{'message': output['message']}]
        else:
            err = "Error status: " + str(result.status_code)
            news = [{'error': err}]
        return news
    
    def get_mna_news(self, company_permalink, num_of_days):
        api_topic = 'M%26A Activity,Investment Activity,Looking to Acquire'
        mna_news = self.get_business_signals(company_permalink, api_topic, num_of_days)
        return mna_news

    def get_financial_news(self, company_permalink, num_of_days):
        api_topic = 'Legal Issues,Reported Profit Increase,Reported Profit Decrease,Reported Revenue Increase,Reported Revenue Decrease,Going Public Activity,Enter Bankruptcy'
        fin_news = self.get_business_signals(company_permalink, api_topic, num_of_days)
        return fin_news

    def get_tech_prod_news(self, company_permalink, num_of_days):
        api_topic = 'Product Launches,Product Upgrade,Business Expansion'
        tech_news = self.get_business_signals(company_permalink, api_topic, num_of_days)
        return tech_news

    def get_leadership_chng_news(self, company_permalink, num_of_days):
        api_topic = 'Board Change Activity,Executive Appointment,Executive Exit,CEO Appointment,COO Appointment,CEO Exit,COO Exit'
        leadership_news = self.get_business_signals(company_permalink, api_topic, num_of_days)
        return leadership_news

    def get_positive_signals(self, company_permalink, num_of_days):
        api_topic = 'Positive Activist Activity,Positive Product Review,Positive Management Feedback,Positive Analyst Outlook,Positive Rating Change,Increase in Share Prices,Decrease in Loans,Increase in Gross Income,Increase in Book Value'
        positive_signals = self.get_business_signals(company_permalink, api_topic, num_of_days)
        return positive_signals
    
    def get_negative_signals(self, company_permalink, num_of_days):
        api_topic = 'Negative Activist Activity,Negative Product Review,Negative Management Feedback,Negative Analyst Outlook,Negative Rating Change,Decrease in Share Prices,Decrease in Loans,Decrease in Gross Income,Decrease in Book Value'
        negative_signals = self.get_business_signals(company_permalink, api_topic, num_of_days)
        return negative_signals
    
    def get_company_news_event(self, company_permalink, num_of_days):
        company_news = self.get_company_news(company_permalink, num_of_days)
        return company_news
