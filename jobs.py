from google.cloud import bigquery
from google.oauth2 import service_account
from rd import RDClient

def bq_service_account_auth(credentials):
    '''
    Returns a client object to call the BigQuery API.
    path: The path to the service account .json credentials file.
    '''
    credentials = service_account.Credentials.from_service_account_info(credentials)
    client = bigquery.Client(credentials=credentials)
    return client

def df_to_bq(table_id, df, write_mode, client):
    '''Takes a dataframe and writes it in a BigQuery table.'''
    if write_mode == 'truncate':
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    elif write_mode == 'append':
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    else:
        print("Invalid write mode value. \nPlease insert 'truncate' or 'append'.")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def load_all(rd_client, bq_client, BQ_PROJECT_ID, BQ_DATASET):
    'Loads all dataframes in a specified BigQuery dataset.'
    # Creating dataframes
    df_pipelines, dict_pipelines = rd_client.pipelines()
    df_custom_fields, dict_custom_fields = rd_client.custom_fields()
    df_stages = rd_client.general_stages(dict_pipelines=dict_pipelines)
    df_sources = rd_client.sources()
    df_products = rd_client.products()
    df_teams = rd_client.teams()
    df_users = rd_client.users()
    df_deal_lost_reasons = rd_client.deal_lost_reasons()
    df_campaigns = rd_client.campaigns()

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
    dict_deals_dfs = rd_client.all_pipeline_deals(dict_custom_fields=dict_custom_fields, dict_pipelines=dict_pipelines)
    dict_dfs.update(dict_deals_dfs)

    for table_name, df in dict_dfs.items():
        table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}'
        if df.shape != (0, 0):
            df_to_bq(table_id=table_id, df=df, write_mode='truncate', client=bq_client)

def update_deals(
        rd_client: RDClient, 
        bq_client,
        pipeline_id: str, 
        deals_table_id: str = None,
        prods_table_id: str = None,
        deals: bool = True, 
        products: bool = False
    ):
    if (deals is True and deals_table_id is None) or (products is True and prods_table_id is None):
        raise ValueError("Unmatching values for table ID's and tables to be updated/loaded.")
    if deals is False and products is False:
        raise ValueError('deals and products are set to False, at least one needs to be True.')
    elif deals is True and products is True:
        output = 'both'
        dict_custom_fields = rd_client.custom_fields(output='dict')
        df_deals, list_deals = rd_client.pipeline_deals(pipeline_id=pipeline_id, dict_custom_fields=dict_custom_fields, output=output)
        df_deals_prods = rd_client.deals_products(pipeline_id=pipeline_id, data=list_deals)
        df_to_bq(
            table_id=deals_table_id, 
            df=df_deals,
            write_mode='truncate',
            client=bq_client
        )
        df_to_bq(
            table_id=prods_table_id, 
            df=df_deals_prods,
            write_mode='truncate',
            client=bq_client
        )
    elif deals is False and products is True:
        df_deals_prods = rd_client.deals_products(pipeline_id=pipeline_id)
        df_to_bq(
            table_id=prods_table_id, 
            df=df_deals_prods,
            write_mode='truncate',
            client=bq_client
        )
    else:
        output = 'df'
        dict_custom_fields = rd_client.custom_fields(output='dict')
        df_deals = rd_client.pipeline_deals(pipeline_id=pipeline_id, dict_custom_fields=dict_custom_fields, output=output)
        df_to_bq(
            table_id=deals_table_id, 
            df=df_deals,
            write_mode='truncate',
            client=bq_client
        )