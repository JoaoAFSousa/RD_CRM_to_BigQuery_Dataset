# Importing libraries
import requests
import pandas as pd
import unicodedata
import re
from google.cloud import bigquery
from google.oauth2 import service_account

# BQ Credentials
credentials = service_account.Credentials.from_service_account_file(
    ''  # Insert here the relative path to your service account .json credentials file.
)
client = bigquery.Client(credentials=credentials)

# Environment variables
RD_CRM_TOKEN = ''  # Insert here your RD CRM account authentication token
API_URL = 'https://crm.rdstation.com/api/v1'
BQ_PROJECT_ID = ''  # Insert here your Google Cloud project ID
BQ_DATASET = ''  # Insert here the BigQuery Dataset ID, where the CRM tables data will be writen

# FUNCTIONS FOR DATA TREATMENT AND LOADING
def text_to_header(text):
    'Takes text input and return in format for BQ headers (it limits string length to 40).'
    # Normalize the string to decompose characters with accents
    normalized_text = unicodedata.normalize('NFKD', text)
    # Remove accents by filtering out characters with Unicode category 'Mn' (Mark, Nonspacing)
    without_accents = ''.join([c for c in normalized_text if not unicodedata.category(c).startswith('M')])
    # Remove any remaining special characters (only keep alphanumeric and spaces)
    cleaned_text = re.sub(r'[^A-Za-z0-9\s]', '', without_accents)
    header_text = cleaned_text.lower().replace(' ', '_')
    return header_text[:40]

def df_to_bq(table_id, df, write_mode):
    '''Takes a dataframe and writes it in a BigQuery table.'''
    if write_mode == 'truncate':
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    elif write_mode == 'append':
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    else:
        print("Invalid write mode value. \nPlease insert 'truncate' or 'append'.")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

# FUNCTIONS FOR EACH ENDPOINT DATA EXTRACTION
def rd_pipelines(API_URL, RD_CRM_TOKEN):
    'Returns dataframe and dictionary ({id: name}) of pipelines from RD CRM account (until 200 pipelines).'
    url = API_URL + '/deal_pipelines'
    params = {
        'token': RD_CRM_TOKEN,
        'limit': 200
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    df_pipelines = pd.json_normalize(response_json).drop(columns=['deal_stages'])
    df_pipelines.columns = [col_name.replace('.', '_') for col_name in df_pipelines.columns]
    for column in df_pipelines.columns:
        if column != 'order':
            df_pipelines[column] = df_pipelines[column].astype(str)
        else:
            df_pipelines[column] = df_pipelines[column].astype(int)
    dict_pipelines = dict(zip(
        df_pipelines['id'],
        df_pipelines['name'].apply(text_to_header)
    ))
    return df_pipelines, dict_pipelines

def rd_pipeline_stages(API_URL, RD_CRM_TOKEN, pipeline_id):
    'Returns pipeline stages.'
    url = API_URL + '/deal_stages'
    params = {
        'token': RD_CRM_TOKEN,
        'deal_pipeline_id': pipeline_id,
        'limit': 12
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    df_pipeline_stages = pd.json_normalize(response_json['deal_stages'])
    df_pipeline_stages.columns = [col_name.replace('.', '_') for col_name in df_pipeline_stages.columns]
    for column in df_pipeline_stages.columns:
        if column == 'created_at' or column == 'updated_at':
            df_pipeline_stages[column] = pd.to_datetime(df_pipeline_stages[column])
        elif column == 'order':
            df_pipeline_stages[column] = df_pipeline_stages[column].astype(int)
        else:
            df_pipeline_stages[column] = df_pipeline_stages[column].astype(str)
    return df_pipeline_stages

def rd_general_stages(API_URL, RD_CRM_TOKEN, dict_pipelines):
    'Returns table with all stages from rd account'
    pipelines_ids = list(dict_pipelines.keys())
    df_stages = pd.DataFrame()
    for pipeline in pipelines_ids:
        df_stages_upd = rd_pipeline_stages(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN, pipeline_id=pipeline)
        df_stages_upd = df_stages_upd[[
            'deal_pipeline_id',
            'deal_pipeline_name', 
            'id', 
            'name', 
            'nickname', 
            'order', 
            'created_at',
            'updated_at',
            'objective',
            'description'
        ]]
        df_stages = pd.concat([df_stages, df_stages_upd], ignore_index=True)
    return df_stages

def rd_sources(API_URL, RD_CRM_TOKEN):
    'Returns dataframe with all sources from account.'
    url = API_URL + '/deal_sources'
    page = 1
    params = {
        'token': RD_CRM_TOKEN,
        'page': page,
        'limit': 200
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    sources_list = response_json['deal_sources']
    while response_json['has_more'] is True:
        page += 1
        params.update({'page': page})
        response = requests.get(url=url, params=params)
        response_json = response.json()
        sources_list.extend(response_json['deal_sources'])
    df_sources = pd.json_normalize(sources_list)
    df_sources.columns = [col_name.replace('.', '_') for col_name in df_sources.columns]
    for column in df_sources.columns:
        if column == 'created_at' or column == 'updated_at':
            df_sources[column] = pd.to_datetime(df_sources[column])
        else:
            df_sources[column] = df_sources[column].astype(str)
    return df_sources

def rd_products(API_URL, RD_CRM_TOKEN):
    'Returns dataframe of products from the account (Max.: 200 - 1st page).'
    url = API_URL + '/products'
    params = {
        'token': RD_CRM_TOKEN,
        'limit': 200
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    products_list  = response_json['products']
    df_products = pd.json_normalize(products_list)
    df_products.columns = [col_name.replace('.', '_') for col_name in df_products.columns]
    for column in df_products.columns:
        if column == 'updated_at' or column == 'created_at':
            df_products[column] = pd.to_datetime(df_products[column])
        elif column == 'base_price':
            df_products[column] = df_products[column].astype(float)
        elif column == 'visible':
            df_products[column] = df_products[column].astype(bool)
        else:
            df_products[column] = df_products[column].astype(str)
    return df_products

def rd_custom_fields(API_URL, RD_CRM_TOKEN):
    'Returns dataframe and dictionary of custom fields from the account.'
    url = API_URL + '/custom_fields'
    params = {'token': RD_CRM_TOKEN}
    response = requests.get(url=url, params=params)
    response_json = response.json()
    df_custom_fields = pd.json_normalize(response_json)
    df_custom_fields.columns = [col_name.replace('.', '_') for col_name in df_custom_fields.columns]
    for column in df_custom_fields.columns:
        if column == 'created_at' or column == 'updated_at':
            df_custom_fields[column] = pd.to_datetime(df_custom_fields[column])
        elif column == 'required' or column == 'allow_new':
            df_custom_fields[column] = df_custom_fields[column].astype(bool)
        elif column == 'order':
            df_custom_fields[column] = df_custom_fields[column].astype(int)
        else:
            df_custom_fields[column] = df_custom_fields[column].astype(str)
    dict_custom_fields = dict(zip(
        df_custom_fields['id'],
        df_custom_fields['label'].apply(text_to_header)
    ))
    return df_custom_fields, dict_custom_fields

def rd_teams(API_URL, RD_CRM_TOKEN):
    'Returns dataframe of teams from the account.'
    url = API_URL + '/teams'
    params = {'token': RD_CRM_TOKEN}
    response = requests.get(url=url, params=params)
    response_json = response.json()
    teams_list = response_json['teams']
    normal_teams_list = []
    for team in teams_list:
        user_ids = []
        usernames = []
        for user in team['team_users']:
            user_ids.append(user['id'])
            usernames.append(user['name'])
        normal_item = {
            'id': team['id'],
            'name': team['name'],
            'created_at': team['created_at'],
            'updated_at': team['updated_at'],
            'user_ids': ', '.join(user_ids),
            'usernames': ', '.join(usernames)
        }
        normal_teams_list.append(normal_item)
    df_teams = pd.json_normalize(normal_teams_list)
    for column in df_teams.columns:
        if column == 'created_at' or column == 'updated_at':
            df_teams[column] = pd.to_datetime(df_teams[column])
        else:
            df_teams[column] = df_teams[column].astype(str)
    return df_teams

def rd_users(API_URL, RD_CRM_TOKEN):
    'Returns dataframe of users from the account.'
    url = API_URL + '/users'
    params = {'token': RD_CRM_TOKEN}
    response = requests.get(url=url, params=params)
    response_json = response.json()
    users_list = response_json['users']
    df_users = pd.json_normalize(users_list)
    df_users.columns = [col_name.replace('.', '_') for col_name in df_users.columns]
    for column in df_users.columns:
        if column == 'created_at' or column == 'updated_at' or column == 'last_login':
            df_users[column] = pd.to_datetime(df_users[column])
        elif column == 'active' or column == 'hidden':
            df_users[column] = df_users[column].astype(bool)
        else:
            df_users[column] = df_users[column].astype(str)
    return df_users

def rd_deal_lost_reasons(API_URL, RD_CRM_TOKEN):
    'Returns dataframe of lost reasons for account.'
    url = API_URL + '/deal_lost_reasons'
    page = 1
    params = {
        'token': RD_CRM_TOKEN,
        'page': page,
        'limit': 200
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    list_lost_reasons = response_json['deal_lost_reasons']
    while response_json['has_more'] is True:
        page += 1
        params.update({'page': page})
        response = requests.get(url=url, params=params)
        response_json = response.json()
        list_lost_reasons.extend(response_json['deal_lost_reasons'])
    df_deal_lost_reasons = pd.json_normalize(list_lost_reasons)
    df_deal_lost_reasons.columns = [col_name.replace('.', '_') for col_name in df_deal_lost_reasons.columns]
    for column in df_deal_lost_reasons.columns:
        if column == 'created_at' or column == 'updated_at':
            df_deal_lost_reasons[column] = pd.to_datetime(df_deal_lost_reasons[column])
        else:
            df_deal_lost_reasons[column] = df_deal_lost_reasons[column].astype(str)
    return df_deal_lost_reasons

def rd_campaigns(API_URL, RD_CRM_TOKEN):
    'Returns dataframe of campaigns from account.'
    url = API_URL + '/campaigns'
    page = 1
    params = {
        'token': RD_CRM_TOKEN,
        'page': page,
        'limit': 200
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    list_campaigns = response_json['campaigns']
    while response_json['has_more'] is True:
        page += 1
        params.update({'page': page})
        response = requests.get(url=url, params=params)
        response_json = response.json()
        list_campaigns.extend(response_json['campaigns'])
    df_campaign = pd.json_normalize(list_campaigns)
    df_campaign.columns = [col_name.replace('.', '_') for col_name in df_campaign.columns]
    for column in df_campaign.columns:
        if column == 'created_at' or column == 'updated_at':
            df_campaign[column] = pd.to_datetime(df_campaign[column])
        else:
            df_campaign[column] = df_campaign[column].astype(str)
    return df_campaign

def rd_pipeline_deals(API_URL, RD_CRM_TOKEN, pipeline_id, dict_custom_fields):
    'Returns dataframe of deals from pipeline (until 10.000 deals).'
    # Data extraction
    url = API_URL + '/deals'
    page = 1
    params = {
        'token': RD_CRM_TOKEN,
        'page': page,
        'deal_pipeline_id': pipeline_id,
        'limit': 200
    }
    response = requests.get(url=url, params=params)
    response_json = response.json()
    list_deals = response_json['deals']
    while response_json['has_more'] is True:
        try:
            page += 1
            params.update({'page': page})
            response = requests.get(url=url, params=params)
            response_json = response.json()
            list_deals.extend(response_json['deals'])
        except KeyError:
            break
    normal_deals_list =[]  # Personalized treatment to normalize json into dataframe
    for deal in list_deals:
        custom_fields = {}  # Extracting custom fields values
        for c_field in deal['deal_custom_fields']:
            custom_fields.update({
                dict_custom_fields[c_field['custom_field_id']]: c_field['value']
            })
        prod_list = []  # List for products field
        for prod in deal['deal_products']:
            prod_list.append(prod['name'])
        contact = {}  # Building 1st contact values
        if len(deal['contacts']) > 0:
            contact.update({'name': deal['contacts'][0]['name']})
            if len(deal['contacts'][0]['emails']) > 0:
                contact.update({'email': deal['contacts'][0]['emails'][0]['email']})
            if len(deal['contacts'][0]['phones']) > 0:
                contact.update({'phone': deal['contacts'][0]['phones'][0]['phone']})
        normal_deal = {
            'id': deal['id'],
            'name': deal['name'],
            'organization': deal.get('organization', {}).get('name'),
            'win': deal['win'],
            'stage': deal['deal_stage']['name'],
            'user': deal['user']['name'],
            'created_at': deal['created_at'],
            'updated_at': deal['updated_at'],
            'closed_at': deal['closed_at'],
            'amount_montly': deal['amount_montly'],
            'amount_unique': deal['amount_unique'],
            'amount_total': deal['amount_total'],
            'source': deal.get('deal_source', {}).get('name'),
            'campaign': deal.get('campaign', {}).get('name'),
            'lost_reason': deal.get('deal_lost_reason', {}).get('name'),
            'products': ', '.join(prod_list),
            'contact_name': contact.get('name', {}),
            'phone': contact.get('phone', {}),
            'email': contact.get('email', {})
        }
        normal_deal.update(custom_fields)
        normal_deals_list.append(normal_deal)
    df_pipeline_deals = pd.json_normalize(normal_deals_list)
    for column in df_pipeline_deals:
        if column == 'created_at' or column == 'updated_at' or column == 'closed_at':
            df_pipeline_deals[column] = pd.to_datetime(df_pipeline_deals[column])
        elif column == 'win':
            df_pipeline_deals[column] = df_pipeline_deals[column].astype(bool)
        elif column == 'amount_montly' or column == 'amount_unique' or column == 'amount_total':
            df_pipeline_deals[column] = df_pipeline_deals[column].astype(float)
        else:
            df_pipeline_deals[column] = df_pipeline_deals[column].astype(str)
    return df_pipeline_deals

def rd_all_pipeline_deals(API_URL, RD_CRM_TOKEN, dict_custom_fields, dict_pipelines):
    'Returns a dictionary of deals dataframes for each deal pipeline from the account.'
    dict_deals_dfs = {}
    for id, deal_pipeline in dict_pipelines.items():
        df_pipeline_deals = rd_pipeline_deals(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN, pipeline_id=id, dict_custom_fields=dict_custom_fields)
        dict_deals_dfs.update({f'deals_{deal_pipeline}': df_pipeline_deals})
    return dict_deals_dfs

def main(API_URL, RD_CRM_TOKEN, BQ_PROJECT_ID, BQ_DATASET):
    'Loads all dataframes in a specified BigQuery dataset.'
    # Creating dataframes
    df_pipelines, dict_pipelines = rd_pipelines(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_pipelines OK.')
    df_custom_fields, dict_custom_fields = rd_custom_fields(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_custom_fields OK.')
    df_stages = rd_general_stages(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN, dict_pipelines=dict_pipelines)
    # print('df_stages OK.')
    df_sources = rd_sources(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_sources OK.')
    df_products = rd_products(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_products OK.')
    df_teams = rd_teams(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_teams OK.')
    df_users = rd_users(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_users OK.')
    df_deal_lost_reasons = rd_deal_lost_reasons(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_deal_lost_reasons OK.')
    df_campaigns = rd_campaigns(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN)
    # print('df_campaigns OK.')

    # Dictionary of all dataframes to load
    dict_dfs = {
        'pipelines': df_pipelines,
        'custom_fields': df_custom_fields,
        'stages': df_stages,
        'sources': df_sources,
        'products': df_products,
        'teams': df_teams,
        'users': df_users,
        'deal_lost_reasons': df_deal_lost_reasons,
        'campaigns': df_campaigns
    }
    # Dictionary of pipeline deals dataframes
    dict_deals_dfs = rd_all_pipeline_deals(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN, dict_custom_fields=dict_custom_fields, dict_pipelines=dict_pipelines)
    # print('deals_dfs OK.')
    dict_dfs.update(dict_deals_dfs)

    for table_name, df in dict_dfs.items():
        table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}'
        df_to_bq(table_id=table_id, df=df, write_mode='truncate')
        # print(table_id, ' loaded.')

main(API_URL=API_URL, RD_CRM_TOKEN=RD_CRM_TOKEN, BQ_PROJECT_ID=BQ_PROJECT_ID, BQ_DATASET=BQ_DATASET)