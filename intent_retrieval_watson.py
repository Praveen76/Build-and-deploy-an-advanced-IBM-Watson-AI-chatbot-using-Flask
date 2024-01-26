from watson_developer_cloud import AssistantV1, WatsonApiException
from utils.resource_reader import ResourceReader

res_reader = ResourceReader()
config = res_reader.read_configuration()

class WAIntentClassification:
    def __init__(self):
        self.WATSON_ASSISTANT = AssistantV1(version=config['WATSON_ASSISTANT']['WA_VERSION'],
                                            username=config['WATSON_ASSISTANT']['WA_USER_NAME'],
                                            password=config['WATSON_ASSISTANT']['WA_PASSWORD'],
                                            url=config['WATSON_ASSISTANT']['target_url'])


    def retrieve_intents(self, text_msg):
        try:
            response = self.WATSON_ASSISTANT.message(workspace_id = config['WATSON_ASSISTANT']['SENTENCE_WORKSPACE_ID'],
                                                     alternate_intents = True,
                                                     input={'text': text_msg}).get_result()
            intent = response['intents'][0]['intent']
            confidence_pct = round(response['intents'][0]['confidence'] * 100, 2)
            output = [intent, confidence_pct]
        except WatsonApiException as exception:
            output = exception
        return output
