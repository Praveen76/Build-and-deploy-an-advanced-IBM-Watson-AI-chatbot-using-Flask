import requests
import json
from requests.auth import HTTPBasicAuth
import time

class Capiq:
    ENDPOINT = 'https://sdk.gds.standardandpoors.com/gdssdk/rest/v2/clientservice.json'
    HEADERS = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept-Encoding': 'gzip,deflate'}
    VERIFY = True

    tempCapIDs = []

    def __init__(self, verify=True):

        self.USERNAME = '<your username>'
        self.PASSWORD = '<Your password>'

    def gdsp(self, identifiers, mnemonics, keydevfilters, properties):
        response = None
        counter = 0
        check = 1
        while check != 0 and counter < 3:
            try:
                response = self.request_capiq(identifiers, mnemonics, keydevfilters, properties, "GDSP")
                check = 0
            except (TimeoutError, ConnectionError):
                check = 1
                counter += 1
                print("TimeOutError or ConnectionError found. Trying again......")
                time.sleep(10)

        return self.process_gdsp_response(response)

    def gdshe(self, identifiers, mnemonics, keydevfilters, properties):
        response = None
        counter = 0
        check = 1
        while check != 0 and counter < 3:
            try:
                response = self.request_capiq(identifiers, mnemonics, keydevfilters, properties, "GDSHE")
                check = 0
            except (TimeoutError, ConnectionError):
                check = 1
                counter += 1
                print("TimeOutError or ConnectionError found. Trying again......")
                time.sleep(10)

        return self.process_gdshe_response(response)

    def request_capiq(self, identifier, mnemonics, keydevfilters, properties, function_name):
        req_array = []

        for i, mnemonic in enumerate(mnemonics):
            req_array.append({
                "function": function_name,
                "identifier": identifier,  # CAP iq id
                "mnemonic": mnemonic,
                "keydevfilters": keydevfilters,
                "properties": properties
            })

        req = {"inputRequests": req_array}

        response = requests.post(
            self.ENDPOINT,
            headers=self.HEADERS,
            data="inputRequests=" + json.dumps(req),
            auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD),
            verify=self.VERIFY
        )

        return response

    def validate_capid(self, response):
        # Validate if we have correct CAP IDs and return an array of Valid IDs.
        validcapids = []

        for ret in response.json()['GDSSDKResponse']:
            error = ret['ErrMsg']
            if len(error) < 1:
                header = ret['Headers'][0]
                identifier = ret['Identifier']
                value = ret['Rows'][0]['Row'][0]
                # print(identifier, header, " = ", value)

                self.tempCapIDs.append(value)

            else:
                error = ret['ErrMsg']
                print("Error", error)

                self.tempCapIDs.append(error)

        for cID in self.tempCapIDs:
            if cID != 'InvalidIdentifier':
                validcapids.append(cID)

        return validcapids

    def process_gdsp_response(self, response):
        # Here we get the raw data for S&P CAP IDs.
        SnPRawKPIData = {}

        for ret in response.json()['GDSSDKResponse']:
            error = ret['ErrMsg']
            if len(error) < 1:
                header = ret['Headers'][0]
                identifier = ret['Identifier']
                value = ret['Rows'][0]['Row'][0]

                SnPRawKPIData[header] = value

            else:
                error = ret['ErrMsg']
                print("WARNING: process gdsp response with ", error)

        return SnPRawKPIData

    def process_gdshe_response(self, response):
        # This is the Peer List from S&P
        sequenceData = []
        count =0
        for ret in response.json()['GDSSDKResponse']:
            error = ret['ErrMsg']
            # print(ret)
            if len(error) < 1:
                header = ret['Headers'][0]
                identifier = ret['Identifier']
                data = []
                for i in range(len(ret['Rows'])):
                    value = ret['Rows'][i]['Row'][0]
                    data.append(value)
                sequenceData.append(data)
            else:
                error = ret['ErrMsg']
                print("process_gdshe_response-Error", error)

            count = count + 1
        return sequenceData
