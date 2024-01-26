import os
import csv
import time
import threading
from cbda.cbda_client import CbdaClient
from ibisworld.ibis_report_getter import IBISWorldReportExtractor
class ThreatsOpportunities:

    def __init__(self):
        get_All_Client_Details = CbdaClient()
        self.company_details = get_All_Client_Details.get_All_Client()
        self.client = IBISWorldReportExtractor(self.company_details[0]['EntityID'])

    def get_all_industry_threats(self):
        thread_max = 15
        single_run = len(self.company_details) / thread_max
        start_index = 0
        last_index = 0
        threat_threads = []
        final_results = []
        for th_item in range(thread_max):
            if th_item == thread_max - 1:
                last_index = len(self.company_details)
            else:
                last_index = last_index + single_run
            threat_thread = threading.Thread(target=self.threats_threading, args=(self.company_details[int(start_index):int(last_index)], th_item, self.client, final_results))
            threat_thread.start()
            threat_threads.append(threat_thread)
            start_index = last_index
        for th in threat_threads:
            th.join()
        industry_threats = []
        for x in final_results:
            for x1 in x:
                industry_threats.append(x1)
        path = r'threats_opportunities\industry_threats.csv'
        try:
            with open(path, 'w+', newline='') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(['Company', 'LOB', 'Threats'])
                writer.writerows(industry_threats)
                csvFile.close()
            return 'file saved successfully'
        except Exception as ex:
            return None

    def threats_threading(self, industry_details, file_count, client, final_results):
        try:
            industry_threats = []
            for industry in industry_details:
                try:
                    industry_threat = []
                    entity_name = industry['CompanyName']
                    entity_id = industry['EntityID']
                    LOB = industry['LOB']
                    sector = industry['Sector_ID']
                    segment = industry['Segment_ID']
                    if entity_id:
                        time.sleep(1)
                        naics_code = client.reset_naics(LOB, sector, segment)
                        data = client.get_industry_threats()
                        industry_threat.append(entity_name)
                        industry_threat.append(industry['LOB'])
                        if data != "Exception fetching data from IbisWorld!":
                            industry_threat.append(data)
                            industry_threats.append(industry_threat)
                except Exception:
                    industry_threats.append(
                        [entity_name, industry['LOB'], 'Threats not found or exception occurred'])
            final_results.append(industry_threats)
            return 'Completed'
        except Exception:
            return 'failed'

    def get_all_industry_opportunities(self):
        client = IBISWorldReportExtractor(self.company_details[0]['EntityID'])
        thread_max = 15
        single_run = len(self.company_details) / thread_max
        start_index = 0
        last_index = 0
        opp_threads = []
        final_results = []
        for th_item in range(thread_max):
            if th_item == thread_max - 1:
                last_index = len(self.company_details)
            else:
                last_index = last_index + single_run
            opp_thread = threading.Thread(target=self.opportunities_threading, args=(self.company_details[int(start_index):int(last_index)], th_item, self.client, final_results))
            opp_thread.start()
            opp_threads.append(opp_thread)
            start_index = last_index
        for th in opp_threads:
            th.join()
        industry_opportunities = []
        for x in final_results:
            for x1 in x:
                industry_opportunities.append(x1)
        path = 'threats_opportunities\industry_opportunities.csv'
        try:
            with open(path, 'w+', newline='') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(['Company', 'LOB', 'Opportunities'])
                writer.writerows(industry_opportunities)
                csvFile.close()
            return 'file saved successfully'
        except Exception as ex:
            return None

    def opportunities_threading(self, industry_details, file_count, client, final_results):
        try:
            industry_opps = []
            for industry in industry_details:
                try:
                    industry_opp = []
                    entity_name = industry['CompanyName']
                    entity_id = industry['EntityID']
                    LOB = industry['LOB']
                    sector = industry['Sector_ID']
                    segment = industry['Segment_ID']
                    if entity_id:
                        time.sleep(1)
                        naics_code = client.reset_naics(LOB, sector, segment)
                        data = client.get_industry_opportutnities()
                        industry_opp.append(entity_name)
                        industry_opp.append(industry['LOB'])
                        if data != "Exception fetching data from IbisWorld!":
                            industry_opp.append(data)
                            industry_opps.append(industry_opp)
                except Exception:
                    industry_opps.append(
                        [entity_name, industry['LOB'], 'Opportunities not found or exception occurred'])
                # que.put(industry_threats)
            final_results.append(industry_opps)
            return 'Completed'
        except Exception:
            return 'failed'
