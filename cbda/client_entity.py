#from cbda.wds_connection import WDSConnection
from utils.wds_connection_advanced import WDSConnectionAdvanced

class ClientEntity:

    def __init__(self):
        #@TODO: Advanced Migration - Remove Old Code After Make Sure it Works with Advanced
        #self.wds = WDSConnection()
        self.wds = WDSConnectionAdvanced()
        try:
            self.CLIENT_COLLECTION = self.wds.get_wds_collection('KCCI-CLIENT-UAT')
            self.REFERENCE_COLLECTION = self.wds.get_wds_collection('KCCI-REFERENCE-UAT')
        except TypeError:
            raise ("Collection {} does not exist".format("KCCI-CLIENT-UAT or KCCI-REFERENCE-UAT"))

        self.client_datatype = 'KCCI-CLIENT'

    def entity_resolution(self, client_id):
        """
        if client and parent are not found in discovery, return with error message
        if client is not found, but parent is found, set client record to parent
        :param client_id: id to be resolved
        :return: either entity id or parent id
        """
        client_id_query = 'EntityID::"%s"' % (client_id)

        client_query = self.wds.discovery.query(self.wds.environment_id, self.CLIENT_COLLECTION, query=client_id_query,
                                                filter='datatype::"{}"'.format(self.client_datatype), count=1).get_result()
        if client_query['results']:
            return [client_id, 'entity']
        else:
            client_id_query = 'Client_ID::"%s"' % (client_id)
            parent_query = self.wds.discovery.query(self.wds.environment_id,
                                                    self.REFERENCE_COLLECTION,
                                                    query=client_id_query,
                                                    filter='datatype::"Client_Indicative"', count=1).get_result()
            if parent_query['results']:
                parent_id = parent_query['results'][0]['Parent_Entity_ID']
                parent_id_query = 'EntityID::"%s"' % (parent_id)
                parent_query = self.wds.discovery.query(self.wds.environment_id,
                                                        self.CLIENT_COLLECTION,
                                                        query=parent_id_query,
                                                        filter='datatype::"{}"'.format(self.client_datatype), count=1).get_result()
                if parent_query['results']:
                    return [parent_id, 'parent']

        return [int(client_id.strip('\"')), 'unknown']
