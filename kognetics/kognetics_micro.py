
class KogneticsMicro:
    def __init__(self):
        self.key = '2685683d26d6eb337e53316f9a0e5c6b'  ## As provided by Kognetics Team
        self.permalink_obj = KogneticsService()  ## To get permalink

    def get_business_signals(self, company_permalink, api_topic):
        api_name = 'BusinessSignals'
        start_dt = '2018-01-01'  ## Dates can be changed to reflect any period for which news is required
        end_dt = '2019-02-20'

        ## Creating the API Url to be targeted as per requirement
        api_url_common = 'https://api.kognetics.com/v1.0/company/'
        api_url_parameters = '{0}?permalink={1}&startDate={2}&endDate={3}&businessSignal={4}&key={5}'.format(
                api_name, company_permalink, start_dt, end_dt, api_topic, self.key)
        req_url = api_url_common + api_url_parameters
        result = requests.get(req_url)
        op = result.json()

        ## Processing the result
        if result and result.status_code == 200:
            try:
                err = op['message']
                info = err
            except KeyError:
                info = op['data']['rows']
        else:
            err = "Error status: " + str(result.status_code)
            info = err
        return info

    def get_mna_news(self, entityID):
        api_topic = 'M%26A Activity,Investment Activity,Looking to Acquire'
        company_permalink = self.permalink_obj.get_permalink(entityID)
        mna_news = self.get_business_signals(company_permalink, api_topic)
        return mna_news

    def get_financial_news(self, entityID):
        api_topic = 'Legal Issues,Reported Profit Increase,Reported Profit Decrease,Reported Revenue Increase,Reported Revenue Decrease,Going Public Activity,Enter Bankruptcy'
        company_permalink = self.permalink_obj.get_permalink(entityID)
        fin_news = self.get_business_signals(company_permalink, api_topic)
        return fin_news

    def get_tech_prod_news(self, entityID):
        api_topic = 'Product Launches,Product Upgrade,Business Expansion'
        company_permalink = self.permalink_obj.get_permalink(entityID)
        tech_news = self.get_business_signals(company_permalink, api_topic)
        return tech_news

    def get_leadership_chng_news(self, entityID):
        api_topic = 'Board Change Activity,Executive Appointment,Executive Exit,CEO Appointment,COO Appointment,CEO Exit,COO Exit'
        company_permalink = self.permalink_obj.get_permalink(entityID)
        leadership_news = self.get_business_signals(company_permalink, api_topic)
        return leadership_news
