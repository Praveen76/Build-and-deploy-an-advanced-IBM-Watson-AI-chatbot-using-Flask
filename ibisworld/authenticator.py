import requests

class OauthAuthenticator(object):
    ACCESS_TOKEN = 'access_token'
    def __init__(self, config_path):
        self.config = self.__get_config(config_path)
        
    def get_access_token(self):
        request_body = {
                        'grant_type': self.config.get('grant_type'), 
                        'client_id':self.config.get('client_id'), 
                        'username':self.config.get('username'),
                        'password':self.config.get('password')
                        }
        response = requests.post(self.config.get('authentication_uri'),request_body)
        access_token = response.json().get(self.ACCESS_TOKEN)
        return access_token
    
    def __get_config(self, config_path):
        #Todo: Add code to get this configuration from config file provided at config_path
        config = {'authentication_uri':'https://services.ibisworld.com/OAuth/Token',
            'client_id':'33581',
            'grant_type':'password',
            'username':'cbda_API (TEST)',
            'password':'SAMAIFY19'
            }
        return config



