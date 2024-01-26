# Copyright 2015 IBM Corp. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from flask import Flask, jsonify
from cbda.cbda_qrc import CbdaQRC
from cbda.cbda_client import CbdaClient
from cbda.client_news import ClientNews
from cbda.overall_recommendation import OverallRecommendation
from cbda.peer_comparison import PeerComparison
from cbda.past_purchase import PastPurchase
from flask_cors import CORS
from cbda.products_and_services import ProductsAndServices
from cbda.client_financials import ClientFinancials
from snp.get_snp_data import GetSnpData
from ibisworld.ibis_report_getter import IBISWorldReportExtractor
from cbda.client_overview import ClientOverview
from cbda.client_highlights import ClientHighlights
from cbda.stock_trailing import StockTrailing
from cbda.NAICS import NAICS
from cbda.client_swot import ClientSWOT
from cbda.client_officers import ClientOfficers
from trkd.get_eikon_data import GetEikonData
from kognetics.get_kognetics_data import GetKogneticsData
from eikon.eikon_service import EikonService
from kognetics.kognetics_service import KogneticsService

# Input parameters
# Input parameters
# A) Collection Name e.g.KCCI-QRC
# B) Input Query parameters e.g. QRC_Index_no
# C) WDS Discovery query itself - QRC_Index_Number::"QRC_Index_No"
# D) Output Parameters - "Description,


app = Flask(__name__)
CORS(app)

# First get Discovery paremters for given Collection name as identified by A).
# I am hard coding for KCCI-QRC right now


# Second define query_string as input from item #B abobe and Invoke WDS query (Item # c) using Quaery parameters defined in Item #2 above
# I am hard coding for input query right now

@app.route('/products/<string:qrc>', methods=['GET'])
def get_qrc(qrc):
    cbda_qrc = CbdaQRC()
    if qrc:
        qrc_json = cbda_qrc.get_qrc(qrc)
        return jsonify(qrc_json)
    else:
        return jsonify({"message": "Please provide a valid qrc index"})

@app.route('/productsByName/<string:qrc>', methods=['GET'])
def get_qrc_by_name(qrc):
    cbda_qrc = CbdaQRC()
    if qrc:
        qrc_json = cbda_qrc.get_qrc_by_name(qrc)
        return jsonify(qrc_json)
    else:
        return jsonify({"message": "Please provide a valid qrc index"})


@app.route('/clients/<string:entity_id>', methods=['GET'])
def get_client(entity_id):
    client = CbdaClient()
    if entity_id:
        data = client.get_client(entity_id)
        return jsonify(data)
    else:
        return jsonify({"message": "Please provide a valid qrc index"})


@app.route('/peers/<string:peer_id>', methods=['GET'])
def get_peers_by_id(peer_id):
    peer = PeerComparison()
    if peer_id:
        data = peer.get_peers_by_id(peer_id)
        return jsonify(data)
    else:
        return jsonify({"message": "Please provide a valid qrc index"})

@app.route('/client_news_chart/<string:entity_id>', methods=['GET'])
def get_client_news_chart(entity_id):
    news = ClientNews()
    if entity_id:
        data = news.get_news_chart(entity_id)
        return jsonify(data)
    else:
        return jsonify({})

@app.route('/client_news/<string:entity_id>', methods=['GET'])
def get_client_news(entity_id):
    news = ClientNews()
    if entity_id:
        data = news.get_news(entity_id)
        return jsonify(data)
    else:
        return jsonify([])


@app.route('/recommendation/<string:entity_id>', methods=['GET'])
def get_recommendation(entity_id):
    recomm = OverallRecommendation()
    if entity_id:
        data = recomm.get_overall_recommendation(entity_id)
        #print(data)
        return jsonify(data)
    else:
        return jsonify({})


@app.route('/peers/CapitalIQ/<string:capitaliqid>', methods=['GET'])
def get_peercomparison_by_capid(capitaliqid):
    if capitaliqid:
        peer = PeerComparison()
        data = peer.get_peercomparison(capitaliqid)
        return jsonify(data)
    else:
        return jsonify({})


@app.route('/clients/<string:entity_id>/peers', methods=['GET'])
def get_peer_ids(entity_id):
    if entity_id:
        peer = PeerComparison()
        data = peer.get_peers(entity_id)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients/<string:entity_id>/pp', methods=['GET'])
def get_past_purchases(entity_id):
    if entity_id:
        past_purchase = PastPurchase()
        data = past_purchase.get_past_purchases(entity_id)
        if data:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients/<string:entity_id>/significant_developement',  methods=['GET'])
def get_financial_insights(entity_id):
  client = CbdaClient()
  capid = client.get_client_capiqid(entity_id)
  snp = GetSnpData()
  data = {'key_products_services': snp.get_key_products(capid), 'technology_advancements': snp.get_technological_advancements(capid),
          'leadership_changes': snp.get_leadership_changes(capid), 'acquisitions': snp.get_acquisitions(capid)}
  return jsonify(data)


@app.route('/clients/<string:entity_id>/financials', methods=['GET'])
def get_client_financials(entity_id):
    if entity_id:
        financials = ClientFinancials()
        data = financials.get_client_financials(entity_id)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients/<string:entity_id>/industry_insights', methods=['GET'])
def get_industry_insights(entity_id):
    if entity_id:
        client = IBISWorldReportExtractor(entity_id)
        glance = client.get_industry_glance()
        key_success_factor = client.get_key_success_factors()
        industry_threats = client.get_industry_threats()
        industry_opportutnities = client.get_industry_opportutnities()
        industry_outlook = client.get_industry_outlook()
        operating_conditions = client.get_operating_conditions()
        executive_summary = client.get_executive_summary_analysis()
        industry_headline = client.get_industry_headline()
        supply_chain = client.get_supply_chain()
        industry_definition = client.get_industry_definition()
        return jsonify({"LOB": client.LOB, "glance": glance, "key_success_factors": key_success_factor,
                        'industry_threats': industry_threats, 'industry_opportutnities': industry_opportutnities,
                        'industry_outlook': industry_outlook, 'operating_conditions': operating_conditions,
                        'executive_summary': executive_summary, 'industry_headline': industry_headline,
                        'industry_definition': industry_definition, 'supply_chain': supply_chain})

    return jsonify({"LOB": client.LOB, "glance": glance, "key_success_factor": key_success_factor,
                    'industry_threats': industry_threats, 'industry_opportutnities': industry_opportutnities,
                    'industry_outlook': industry_outlook, 'operating_conditions': operating_conditions,
                    'executive_summary': executive_summary, 'industry_headline': industry_headline,
                    'industry_definition': industry_definition, 'supply_chain': supply_chain})

@app.route('/clients/<string:entity_id>/overview', methods=['GET'])
def get_client_overview(entity_id):
    if entity_id:
        overview = ClientOverview()
        data = overview.get_client_overview(entity_id)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients/<string:entity_id>/highlights', methods=['GET'])
def get_client_highlights(entity_id):
    if entity_id:
        overview = ClientHighlights()
        data = overview.get_client_highlights(entity_id)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients/<string:entity_id>/stock_trailing/<string:months>', methods=['GET'])
def get_client_stock_trailing(entity_id, months):
    if entity_id:
        trailing = StockTrailing()
        data = trailing.get_client_stock_trailing(entity_id, months)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients/<string:entity_id>/strength_weakness', methods=['GET'])
def get_client_strength_weakness(entity_id):
    if entity_id:
        swot = ClientSWOT()
        data = swot.get_client_strength_weakness(entity_id)
        if len(data) > 0:
            return jsonify(data)

    return jsonify({})

@app.route('/client_officers/<string:client_id>', methods=['GET'])
def get_client_officers(client_id):
    if client_id:
        overview = ClientOfficers()
        data = overview.get_client_officers(client_id)
        print (data)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/news_headline/<string:ticker>', methods=['GET'])
def get_client_news_headline(ticker):
    '''

    :param ticker: Example MSFT.O
    :return: Company news headlines as a dictionary

    This is only for demonstrating Thomson Eikon Authentication end-to-end
    It may or may not be utilized on SAM AI PoC
    '''
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_organisation(ticker)  # Microsoft 'MSFT.O'
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/recent_news_and_events/<string:ticker>', methods=['GET'])
def get_recent_news_and_events(ticker):
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_recent_news_and_events(ticker)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/litigation/<string:ticker>', methods=['GET'])
def get_litigation(ticker):
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_litigation_developments(ticker)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/management_changes/<string:ticker>', methods=['GET'])
def get_management_changes(ticker):
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_management_change_developments(ticker)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])


@app.route('/bankruptcy_changes/<string:ticker>', methods=['GET'])
def get_bankruptcy_changes(ticker):
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_bankruptcy_related_developments(ticker)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])


@app.route('/business_expansion_changes/<string:ticker>', methods=['GET'])
def get_business_expansion_developments(ticker):
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_business_expansion_changes(ticker)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])


@app.route('/mergers_and_acquisitions/<string:ticker>', methods=['GET'])
def get_mergers_and_acquisition(ticker):
    if ticker:
        eikon = GetEikonData()
        data = eikon.get_mergers_and_acquisition_developments(ticker)
        if len(data) > 0:
            return jsonify(data)
        else:
            return jsonify([])
    else:
        return jsonify([])

@app.route('/clients_by_company/<string:companyname>', methods=['GET'])
def get_entity_id(companyname):
    if companyname:
        data = CbdaClient()
        companyid = data.get_entityid(companyname)
        if (len(companyid)>0):
            return companyid
        else:
            return 'Company id not available'
    else:
        return ' '

@app.route('/premier/topics_k/<string:entity_id>', methods=['GET'])
def get_kognetics_topics(entity_id):
    if entity_id:
        permalink_obj = KogneticsService()
        permalink = permalink_obj.get_permalink(entity_id)  ## To get permalink for corresponding entity ID

        if not(permalink == 'no permalink retrieved'):
            num_of_days = 365
            kog_news = GetKogneticsData()
            data1 = kog_news.get_mna_news(permalink, num_of_days)
            data2 = kog_news.get_financial_news(permalink, num_of_days)
            data3 = kog_news.get_tech_prod_news(permalink, num_of_days)
            data4 = kog_news.get_leadership_chng_news(permalink, num_of_days)
            data5 = kog_news.get_positive_signals(permalink, num_of_days)
            data6 = kog_news.get_negative_signals(permalink, num_of_days)
            data7 = kog_news.get_company_news_event(permalink, num_of_days)

            data = {'Merger_Acquistion': data1, 'Financial_News': data2,
                    'Tech_Products': data3, 'Leadership_News': data4,
                    'Positive_Signals': data5, 'Negative_Signals': data6,
                    'Company_News': data7}
            return jsonify(data)
        else:
            data = {'Output': 'Company Not Found In Kognetics DB'}
            return jsonify(data)
    else:
        return jsonify({})


@app.route('/premier/topics_te/<string:entity_id>', defaults={'time_range': None}, methods=['GET'])
@app.route('/premier/topics_te/<string:entity_id>/<int:time_range>', methods=['GET'])
def get_eikon_topics(entity_id, time_range):
    if entity_id:
        rics_resolution = EikonService()
        rics = rics_resolution.get_ric(entity_id)

        #@TODO place default timespan value in a config file
        days_range = 92
        if time_range:
            days_range = time_range if time_range > 0 else 92

        eikon = GetEikonData()
        data1 = eikon.get_recent_business_events_pao(rics, days_range)
        data2 = eikon.get_key_initiatives_overview_pao(rics, days_range)
        data3 = eikon.get_bankruptcy_related_developments(rics, days_range)
        data4 = eikon.get_business_expansion_changes(rics, days_range)
        data5 = eikon.get_litigation_developments(rics, days_range)
        data6 = eikon.get_mergers_and_acquisition_developments(rics, days_range)

        data= {'Recent_Business_Events':data1, 'Key_Initiatives_Overview': data2, 'Bankrupcy': data3,
              'Expansion': data4, 'Litigation':data5, 'Merger_Acquisition': data6}
        return jsonify(data)
    else:
        return jsonify({})


port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port))


