from utils.wds_connection_advanced import WDSConnectionAdvanced


class KogneticsService:

    def __init__(self):
      self.wds = WDSConnectionAdvanced()
      self.collection_id = self.wds.get_wds_collection('KCCI-KOGNETICS')

    def get_permalink(self, entity_id=''):
      if entity_id == '':
        return 'No entity_id provided'
      filter = 'Entity_ID::\"' + str(entity_id)+'\"'

      query_results = self.wds.discovery.query(self.wds.environment_id,
                                               self.collection_id,
                                               filter=filter, count=1).get_result()
      if len(query_results['results']) > 0:
        return query_results['results'][0]['Permalink']
      return 'no permalink retrieved'

    
