import http.client
import json
import requests
from types import SimpleNamespace
import singer

LOGGER = singer.get_logger()

def get_oauth_token(client_secret="", client_id=""):

    postData = {
        "client_secret": client_secret,
        "client_id": client_id,
        "grant_type": "client_credentials"
    }
    r = requests.post('https://api.criteo.com/oauth2/token', postData)

    return json.loads(r.text)

def mc_fetch(token, path, queryParams = None):
    headers = {
        'Authorization': token
    }

    path = 'https://api.criteo.com/' + path

    r = requests.get(path, queryParams, headers=headers)

    result = json.loads(r.text)
    return result


def mc_get_audiences(token, advertiser_id):

    headers = {
        'Authorization': token
    }

    r = requests.get('https://api.criteo.com/legacy/marketing/v1/audiences', {"advertiserId": advertiser_id}, headers=headers)

    result = json.loads(r.text)
    return result['audiences']

def mc_sync_statistics(token, stats_query):

    stats_query['Format'] = 'Csv'
    stats_query['startDate'] = stats_query['start_date']
    stats_query['endDate'] = stats_query['end_date']

    headers = {
        'Authorization': token
    }

    path = 'https://api.criteo.com' + '/legacy/marketing/v1/statistics'

    finalQuery = {
        'statsQuery': json.dumps(stats_query),
        'startDate': stats_query['start_date'],
        'endDate': stats_query['end_date'],
    }
    r = requests.post(path, finalQuery, headers=headers)

    return r.text

def mc_sync(table, token, advertiser_id):

    if table == 'CampaignsApi.get_campaigns':
        return mc_fetch(token, 'legacy/marketing/v1/campaigns', {'advertiserIds': advertiser_id})
    if table == 'CategoriesApi.get_categories':
        return mc_fetch(token, 'legacy/marketing/v1/categories', {'advertiserIds': advertiser_id})
    if table == 'BudgetsApi.get':
        return mc_fetch(token, 'legacy/marketing/v1/budgets', {'advertiserIds': advertiser_id})
    if table == 'SellersV2Api.get_advertisers':
        return []
    if table == 'PortfolioApi.get_portfolio':
        # Includes sensitive information
        return []
    if table == 'SellersV2Api.get_seller_budgets':
        return mc_fetch(token, 'legacy/marketing/v2/crp/budgets', {'advertiserId': advertiser_id})
    if table == 'SellersV2Api.get_seller_campaigns':
        return mc_fetch(token, 'legacy/marketing/v2/crp/seller-campaigns', {'advertiserId': advertiser_id})
    if table == 'SellersV2Api.get_sellers':
        return mc_fetch(token, 'legacy/marketing/v2/crp/sellers', {'advertiserId': advertiser_id})

    ###
    if table == '':
        return mc_fetch(token, 'legacy/marketing/', {'advertiserId': advertiser_id})
    return []

def mc_get_categories(token, advertiser_id):
    result = mc_fetch(token, advertiser_id, 'legacy/marketing/v1/campaigns')
    return result

def mc_get_campaigns(token, advertiser_id):
    result = mc_fetch(token, advertiser_id, 'legacy/marketing/v1/campaigns')
    return result

