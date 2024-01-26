from cbda.peer_comparison import PeerComparison
from cbda.cbda_client import CbdaClient
import pandas as pd
import numpy as np

class ClientSWOT:
    def __init__(self):
        self.peer = PeerComparison()
        self.client = CbdaClient()

    def get_client_strength_weakness(self, entity_id):
        peer_data = self.peer.get_peer_data(entity_id)[0]
        return self.strength_weakness(peer_data, entity_id)
    def strength_weakness(self, peer_data, entity_id):
        weakness = []
        strengths = []
        comps = []
        for peer in peer_data:
            temp_dict = dict()
            temp_dict["CompanyId"] = peer["CompanyId"]
            temp_dict["CompanyName"] = peer["CompanyName"]

            for measure in peer["Data"]:
                temp_dict[measure["Measure"]] = measure["Percent"]
                comps.append(temp_dict)

                temp_dict = {}
                temp_dict["CompanyId"] = peer["CompanyId"]
                temp_dict["CompanyName"] = peer["CompanyName"]

        df = pd.DataFrame(comps)
        columns = ["Asset Efficiency", "Cash Management", "Cost Management", "Profitability", "Size And Growth"]

        # over performer if largest or second largest
        for column in columns:
            df_sorted = df.sort_values(column)
            df_sorted = df_sorted[np.isfinite(df_sorted[column])]

            if df_sorted.tail(n=1)["CompanyId"].values[0] == entity_id:
                over_performing_kpis = dict()
                over_performing_kpis[column] = df_sorted.tail(n=1)[column].values[0]
                strengths.append(over_performing_kpis)

            if df_sorted.head(n=1)["CompanyId"].values[0] == entity_id:
              under_performing_kpis = dict()
              under_performing_kpis[column] = df_sorted.head(n=1)[column].values[0]
              weakness.append(under_performing_kpis)

        return {"strength": strengths, "weakness": weakness}

