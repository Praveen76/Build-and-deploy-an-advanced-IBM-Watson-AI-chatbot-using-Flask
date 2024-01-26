# from trkd.eikon import ThomsonEikon
from trkd.eikon import ThomsonEikon
from datetime import date, timedelta

class GetEikonData:
    def __init__(self):
        self.eikonObj = ThomsonEikon()
        self.significance_one_two_three = "1 2 3"
        self.significance_two_three = "2 3"
        self.significant_development_url = 'http://api.trkd.thomsonreuters.com/api/SignificantDevelopments/SignificantDevelopments.svc/REST/SignificantDevelopments_1/GetSignificantDevelopments_1'
        self.search_url = 'http://api.trkd.thomsonreuters.com/api/Search/Search.svc/REST/Organisation_1/GetOrganisation_1'

    def get_organisation(self, symbol):
        organisation_req_message = {"GetOrganisation_Request_1": {
            "Query": [{"Search": {"Include": True, "StringValue": [{"Value": symbol}]}}]}}
        org_result = self.eikonObj.get_client_data(organisation_req_message, self.search_url)

        return org_result.json()

    def get_recent_news_and_events(self, symbol):
        recent_news_and_events_msg = {
            "GetSignificantDevelopments_Request_1": {
                "FindRequest": {
                    "CompanyIdentifiers_typehint": [
                        "CompanyIdentifiers"
                    ],
                    "CompanyIdentifiers": [
                        {
                            "RIC": {
                                "Value": symbol
                            }
                        }
                    ],
                    "Significance": self.significance_one_two_three,
                    "MaxNumberOfItems": 10
                }
            }
        }

        strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url)
        return self.extract_data(strat_result.json())

    def get_mergers_and_acquisition_developments(self, symbol, days_range = 92):
        acquisition = ["207"]  # mergers/acquisitions

        days_to_subtract = days_range  # Arbitrary number corresponding to ~ 3 months
        end_date = date.today()
        start_date = end_date - timedelta(days=days_to_subtract)

        recent_news_and_events_msg = {
            "GetSignificantDevelopments_Request_1": {
                "FindRequest": {
                    "CompanyIdentifiers_typehint": [
                        "CompanyIdentifiers"
                    ],
                    "CompanyIdentifiers": [
                        {
                            "RIC": {
                                "Value": symbol
                            }
                        }
                    ],
                    "Significance": self.significance_one_two_three,
                    "Topics": acquisition[0],
                    "StartDate": "{}T00:00:00".format(start_date),
                    "EndDate": "{}T00:00:00".format(end_date),
                    "MaxNumberOfItems": 3
                }
            }
        }

        strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url)

        return self.extract_data(strat_result.json())

    def get_litigation_developments(self, symbol, days_range=92):
        corp_litigation_class_Act_lawsuit = ["243 244"]  # litigation

        days_to_subtract = days_range  # Arbitrary number corresponding to ~ 3 months
        end_date = date.today()
        start_date = end_date - timedelta(days=days_to_subtract)

        recent_news_and_events_msg = {
            "GetSignificantDevelopments_Request_1": {
                "FindRequest": {
                    "CompanyIdentifiers_typehint": [
                        "CompanyIdentifiers"
                    ],
                    "CompanyIdentifiers": [
                        {
                            "RIC": {
                                "Value": symbol
                            }
                        }
                    ],
                    "Significance": self.significance_one_two_three ,
                    "Topics": corp_litigation_class_Act_lawsuit[0],
                    "StartDate": "{}T00:00:00".format(start_date),
                    "EndDate": "{}T00:00:00".format(end_date),
                    "MaxNumberOfItems": 3
                }
            }
        }

        strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url)

        return self.extract_data(strat_result.json())

    def get_management_change_developments(self, symbol):
        management_change = ["210"]  # officer changes
        recent_news_and_events_msg = {
            "GetSignificantDevelopments_Request_1": {
                "FindRequest": {
                    "CompanyIdentifiers_typehint": [
                        "CompanyIdentifiers"
                    ],
                    "CompanyIdentifiers": [
                        {
                            "RIC": {
                                "Value": symbol
                            }
                        }
                    ],
                    "Significance": self.significance_one_two_three,
                    "Topics": management_change[0],
                    "MaxNumberOfItems": 3
                }
            }
        }
        strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url)

        return self.extract_data(strat_result.json())

    def get_bankruptcy_related_developments(self, symbol, days_range=92):
        bankruptcy_change = ["228"]  # bankruptcy changes

        days_to_subtract = days_range  # Arbitrary number corresponding to ~ 3 months
        end_date = date.today()
        start_date = end_date - timedelta(days=days_to_subtract)

        recent_news_and_events_msg = {
            "GetSignificantDevelopments_Request_1": {
                "FindRequest": {
                    "CompanyIdentifiers_typehint": [
                        "CompanyIdentifiers"
                    ],
                    "CompanyIdentifiers": [
                        {
                            "RIC": {
                                "Value": symbol
                            }
                        }
                    ],
                    "Significance": self.significance_one_two_three ,
                    "Topics": bankruptcy_change[0],
                    "StartDate": "{}T00:00:00".format(start_date),
                    "EndDate": "{}T00:00:00".format(end_date),
                    "MaxNumberOfItems": 3
                }
            }
        }

        strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url)

        return self.extract_data(strat_result.json())

    def get_business_expansion_changes(self, symbol, days_range=92):
        jv_strat_alliances_biz_deals = ["204", "216"]  # business expansion

        days_to_subtract = days_range  # Arbitrary number corresponding to ~ 3 months
        end_date = date.today()
        start_date = end_date - timedelta(days=days_to_subtract)

        recent_news_and_events_msg = {
            "GetSignificantDevelopments_Request_1": {
                "FindRequest": {
                    "CompanyIdentifiers_typehint": [
                        "CompanyIdentifiers"
                    ],
                    "CompanyIdentifiers": [
                        {
                            "RIC": {
                                "Value": symbol
                            }
                        }
                    ],
                    "Significance": self.significance_one_two_three ,
                    "Topics": jv_strat_alliances_biz_deals[0] + " " + jv_strat_alliances_biz_deals[1],
                    "StartDate": "{}T00:00:00".format(start_date),
                    "EndDate": "{}T00:00:00".format(end_date),
                    "MaxNumberOfItems": 10
                }
            }
        }

        strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url)

        return self.extract_data(strat_result.json())


    def get_recent_business_events_pao(self, symbol, days_range):
        days_to_subtract = days_range # Arbitrary number corresponding to ~ 3 months

        end_date = date.today()
        start_date = end_date - timedelta(days=days_to_subtract)

        pao_topics = {
            "Equity Investment (Transaction / Buy)" : "219 207 222",
            "Products (Growth / New Products)" : "201 253",
            "Management Changes (Officer Changes)" : "210"
        }
        context = list()

        for topic_name, topic_code in pao_topics.items():
            recent_news_and_events_msg = {
                "GetSignificantDevelopments_Request_1": {
                    "FindRequest": {
                        "CompanyIdentifiers_typehint": [
                            "CompanyIdentifiers"
                        ],
                        "CompanyIdentifiers": [
                            {
                                "RIC": {
                                    "Value": symbol
                                }
                            }
                        ],
                        "Significance": self.significance_two_three,
                        "Topics": str(topic_code),
                        # @TODO Understand Eikon date format and fix this
                        "StartDate": "{}T00:00:00".format(start_date),
                        "EndDate": "{}T00:00:00".format(end_date),
                        "MaxNumberOfItems": 3
                    }
                }
            }

            strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url).json()
            developments = strat_result['GetSignificantDevelopments_Response_1']['FindResponse']

            if (developments):
                developments = developments['Development']
                for development in developments:
                    temp_dict = dict()

                    temp_dict["Company Name"] = development["Xrefs"]["Name"]
                    temp_dict["Headline"] = development["Headline"]
                    temp_dict["Description"] = development["Description"]
                    temp_dict["cbda Topic"] = str(topic_name)

                    development_id = development["Xrefs"]["DevelopmentId"]
                    temp_dict["DevelopmentID"] = development_id
                    temp_dict["Development URL"] = ""

                    context.append(temp_dict)

        return context

    def get_key_initiatives_overview_pao(self, symbol, days_range=92):
        days_to_subtract = days_range  # Arbitrary number corresponding to ~ 3 months
        end_date = date.today()
        start_date = end_date - timedelta(days=days_to_subtract)

        pao_topics = {
            "Customer Centricity": {"topic" : "253", "initiative": "Service Transformation and Technology"},
            "Corporate Strategy": {"topic": "253", "initiative": "Business Model"}
        }
        context = list()

        for topic_name, topic_code in pao_topics.items():
            recent_news_and_events_msg = {
                "GetSignificantDevelopments_Request_1": {
                    "FindRequest": {
                        "CompanyIdentifiers_typehint": [
                            "CompanyIdentifiers"
                        ],
                        "CompanyIdentifiers": [
                            {
                                "RIC": {
                                    "Value": symbol
                                }
                            }
                        ],
                        "Significance": self.significance_two_three ,
                        "Topics": str(topic_code["topic"]),
                        # @TODO Understand Eikon date format and fix this
                        "StartDate": "{}T00:00:00".format(start_date),
                        "EndDate": "{}T00:00:00".format(end_date),
                        "MaxNumberOfItems": 3
                    }
                }
            }

            strat_result = self.eikonObj.get_client_data(recent_news_and_events_msg, self.significant_development_url).json()
            developments = strat_result['GetSignificantDevelopments_Response_1']['FindResponse']

            if (developments):
                developments = developments['Development']
                for development in developments:
                    temp_dict = dict()

                    temp_dict["Strategic Initiative"] = topic_name
                    temp_dict["Key Initiative"] = topic_code["initiative"]
                    temp_dict["Company Name"] = development["Xrefs"]["Name"]
                    temp_dict["Headline"] = development["Headline"]
                    temp_dict["Description"] = development["Description"]
                    temp_dict["cbda Topic"] = str(topic_name)

                    development_id = development["Xrefs"]["DevelopmentId"]
                    temp_dict["DevelopmentID"] = development_id
                    temp_dict["Development URL"] = ""

                    context.append(temp_dict)

        return context

    def extract_data(self, response):
        try:
            return response['GetSignificantDevelopments_Response_1']['FindResponse']['Development']
        except Exception as e:
            return []
