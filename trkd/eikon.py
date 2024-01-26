import sys
import requests
import json

class ThomsonEikon:

    def __init__(self, verify=True):
        # self.USERNAME = 'trkd-demo-im@thomsonreuters.com'
        # self.APP_ID = 'trkddemoappim'
        # self.PASSWORD = 'z1w2c36qo'

        self.USERNAME = 'trkd-demo-wm@thomsonreuters.com'
        self.APP_ID = 'trkddemoappwm'
        self.PASSWORD = 'y7p6w59jl'
        self.ENDPOINT = 'https://api.trkd.thomsonreuters.com/api/TokenManagement/TokenManagement.svc/REST/Anonymous/TokenManagement_1/CreateServiceToken_1'

    def send_request(self, auth_url, auth_msg, header):
        result = None
        try:
            result = requests.post(auth_url, data=json.dumps(auth_msg), headers=header)
            if result.status_code is not 200:
                print('Request fail')
                print('response status {}'.format(result.status_code))

                if result.status_code == 500:
                    print('Error: '.format(result.json()))
                result.raise_for_status()
        except requests.exceptions.RequestException as e:
            print('Exception!')
            print(e)
            # sys.exit(1)

        return result

    def create_auth(self, username, password, app_id):
        token = None
        auth_url = self.ENDPOINT
        auth_msg = {'CreateServiceToken_Request_1': {'ApplicationID': app_id, 'Username': username, 'Password': password}}
        header = {'content-type': 'application/json;charset=utf-8'}

        # print('.::. Sending Authentication request message to TRKD .::.')

        auth_result = self.send_request(auth_url, auth_msg, header)

        if auth_result and auth_result.status_code == 200:
            # print('Authentication Success')
            # print('response status {}'.format(auth_result.status_code))
            token = auth_result.json()['CreateServiceToken_Response_1']['Token']

        return token


    def get_client_data(self, req_message, req_url):
        req_message = req_message
        req_url = req_url
        user = self.USERNAME
        password = self.PASSWORD
        app_id = self.APP_ID
        token = self.create_auth(user, password, app_id)

        header = {'content-type': 'application/json;charset=utf-8',
               'X-Trkd-Auth-ApplicationID': self.APP_ID,
               'X-Trkd-Auth-Token': token}

        result = self.send_request(req_url, req_message, header)

        # if result and result.status_code == 200:
        #     print('Code:200 - successfully received response message')

        return result







