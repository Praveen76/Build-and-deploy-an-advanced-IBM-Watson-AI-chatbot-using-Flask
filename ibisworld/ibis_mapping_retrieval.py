from utils.wds_connection_advanced import WDSConnectionAdvanced

class IbisMappingRetrieval:
    def __init__(self):
        self.wds = WDSConnectionAdvanced()
        self.mapping_collection_id = self.wds.get_wds_collection("KCCI-IBIS-UAT")

    def get_naics_code(self, LOB_Name, Sector, Segment):
        filter = 'LOB_Name::\"' + LOB_Name + '\", Sector:\"' + str(Sector) + '\", Segment:\"' + str(Segment) + '\"';

        query_results = self.wds.discovery.query(self.wds.environment_id,
                                                 self.mapping_collection_id,
                                                 filter=filter, count=1).get_result()

        if len(query_results['results']) > 0:
            return query_results['results'][0]["Code"]
        return None
