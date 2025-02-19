import requests
import pandas as pd
import numpy as np
import unicodedata
import re


def text_to_snakecase(text):
    'Takes text input and returns in snakecase (it limits string length to 40).'
    # Normalize the string to decompose characters with accents
    normalized_text = unicodedata.normalize('NFKD', text)
    # Remove accents by filtering out characters with Unicode category 'Mn' (Mark, Nonspacing)
    without_accents = ''.join([c for c in normalized_text if not unicodedata.category(c).startswith('M')])
    # Remove any remaining special characters (only keep alphanumeric and spaces)
    cleaned_text = re.sub(r'[^A-Za-z0-9\s]', '', without_accents)
    header_text = cleaned_text.lower().replace(' ', '_')
    return header_text[:40]

class RDClient:
    def __init__(self, token=None):
        self.url = 'https://crm.rdstation.com/api/v1'
        self.token = token

        if self.token is None:
            raise ValueError('Please, insert an access token.')
        else:
            test_url = self.url + '/token/check'
            params = {'token': self.token}
            test_token = requests.get(test_url, params=params)
            if test_token.status_code != 200:
                raise PermissionError(f'Invalid access token! RD response: {test_token.text}')   

    def custom_fields(self, output: str = 'both'):
        'Returns dataframe and dictionary of custom fields from the account.'
        valid_out = ['both', 'df', 'dict']
        if valid_out.count(output) != 1:
            raise ValueError(f'Invalid output value called! Please call one of the options: {valid_out}')
        url = self.url + '/custom_fields'
        params = {'token': self.token}
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
            df_custom_fields['label'].apply(text_to_snakecase)
        ))
        if output == 'both':
            return df_custom_fields, dict_custom_fields
        elif output == 'df':
            return df_custom_fields
        elif output == 'dict':
            return dict_custom_fields

    def pipelines(self, output: str = 'both'):
        'Returns dataframe and dictionary ({id: name}) of pipelines from RD CRM account (until 200 pipelines).'
        valid_out = ['both', 'df', 'dict']
        if valid_out.count(output) != 1:
            raise ValueError(f'Invalid output value called! Please call one of the options: {valid_out}')
        url = self.url + '/deal_pipelines'
        params = {
            'token': self.token,
            'limit': 200
        }
        response = requests.get(url=url, params=params)
        response_json = response.json()
        df_pipelines = pd.json_normalize(response_json).drop(columns=['deal_stages'])
        df_pipelines.columns = [col_name.replace('.', '_') for col_name in df_pipelines.columns]
        for column in df_pipelines.columns:
            if column == 'order':
                df_pipelines[column] = df_pipelines[column].astype(int)
            else:  
                df_pipelines[column] = df_pipelines[column].astype(str)
        dict_pipelines = dict(zip(
            df_pipelines['id'],
            df_pipelines['name'].apply(text_to_snakecase)
        ))
        if output == 'both':
            return df_pipelines, dict_pipelines
        elif output == 'df':
            return df_pipelines
        elif output == 'dict':
            return dict_pipelines

    def pipeline_stages(self, pipeline_id):
        'Returns pipeline stages.'
        url = self.url + '/deal_stages'
        params = {
            'token': self.token,
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

    def general_stages(self, dict_pipelines):
        'Returns table with all stages from rd account'
        pipelines_ids = list(dict_pipelines.keys())
        df_stages = pd.DataFrame()
        for pipeline in pipelines_ids:
            df_stages_upd = self.pipeline_stages(pipeline_id=pipeline)
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

    def sources(self):
        'Returns dataframe with all sources from account.'
        url = self.url + '/deal_sources'
        page = 1
        params = {
            'token': self.token,
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
    
    def products(self):
        'Returns dataframe of products from the account (Max.: 200 - 1st page).'
        url = self.url + '/products'
        params = {
            'token': self.token,
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


    def teams(self):
        'Returns dataframe of teams from the account.'
        url = self.url + '/teams'
        params = {'token': self.token}
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

    def users(self):
        'Returns dataframe of users from the account.'
        url = self.url + '/users'
        params = {'token': self.token}
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

    def deal_lost_reasons(self):
        'Returns dataframe of lost reasons for account.'
        url = self.url + '/deal_lost_reasons'
        page = 1
        params = {
            'token': self.token,
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

    def campaigns(self):
        'Returns dataframe of campaigns from account.'
        url = self.url + '/campaigns'
        page = 1
        params = {
            'token': self.token,
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

    def pipeline_deals(self, pipeline_id, dict_custom_fields, output: str = 'df'):
        'Returns dataframe of deals from pipeline (until 10.000 deals).'
        valid_out = ['both', 'df', 'list']
        if valid_out.count(output) != 1:
            raise ValueError(f'Invalid output value called! Please call one of the options: {valid_out}')
        # Data extraction
        url = self.url + '/deals'
        page = 1
        params = {
            'token': self.token,
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
        if output == 'list':
            return list_deals
        else:
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
                elif column == 'amount_montly' or column == 'amount_unique' or column == 'amount_total':
                    df_pipeline_deals[column] = df_pipeline_deals[column].astype(float)
                elif df_pipeline_deals[column].notna().any() == np.False_ and list(df_pipeline_deals.columns).index(column) > 18:
                    df_pipeline_deals.drop(columns=column, inplace=True)  # Drop custom fields with all null values
        if output == 'df':
            return df_pipeline_deals
        elif output == 'both':
            return df_pipeline_deals, list_deals

    def all_pipeline_deals(self, dict_custom_fields, dict_pipelines):
        'Returns a dictionary of deals dataframes for each deal pipeline from the account.'
        dict_deals_dfs = {}
        for id, deal_pipeline in dict_pipelines.items():
            df_pipeline_deals = self.pipeline_deals(pipeline_id=id, dict_custom_fields=dict_custom_fields)
            dict_deals_dfs.update({f'deals_{deal_pipeline}': df_pipeline_deals})
        return dict_deals_dfs
    
    def deals_products(self, pipeline_id, data:list=None):
        if data is None:
            url = self.url + '/deals'
            page = 1
            params = {
                'token': self.token,
                'pipeline_id': pipeline_id,
                'product_presence': 'true',
                'page': page,
                'limit': 200
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                response_json = response.json()
                data = response_json['deals']
                while response_json['has_more'] == True:
                    page += 1
                    params.update({'page': page})
                    try:
                        response = requests.get(url, params=params)
                        response_json = response.json()
                        data.extend(response_json['deals'])
                    except KeyError:
                        break
            else:
                raise ValueError(f'API response: {response.text}')
        normal_data = []
        for deal in data:
            if len(deal['deal_products']) > 0:
                for prod in deal['deal_products']:
                    item = {
                        'deal_id': deal['id'],
                        'product_id': prod['product_id'],
                        'name': prod['name'],
                        'description': prod['description'],
                        'base_price': prod['base_price'],
                        'created_at': prod['created_at'],
                        'updated_at': prod['updated_at'],
                        'price': prod['price'],
                        'amount': prod['amount'],
                        'recurrence': prod['recurrence'],
                        'discount': prod['discount'],
                        'discount_type': prod['discount_type'],
                        'total': prod['total']
                    }
                    normal_data.append(item)  
        df_deals_prods = pd.DataFrame(normal_data)
        return  df_deals_prods