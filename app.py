from flask import Flask, request
from rd import RDClient
from jobs import load_all, bq_service_account_auth, update_deals
from google.cloud import bigquery
import os

app = Flask(__name__)

# Flask route
@app.route('/')
def home():
    return {'message': 'Service is running'}, 200

@app.route('/load', methods=['POST'])
def handle_load_request():
    data = request.get_json()
    RD_CRM_TOKEN = data.get('RD_CRM_TOKEN')
    BQ_PROJECT_ID = data.get('BQ_PROJECT_ID')
    BQ_DATASET = data.get('BQ_DATASET')
    # Credentials
    bq_client = bigquery.Client()
    rd_client = RDClient(RD_CRM_TOKEN)
    load_all(rd_client, bq_client=bq_client, BQ_PROJECT_ID=BQ_PROJECT_ID, BQ_DATASET=BQ_DATASET)
    return {'message': 'Job executed successfully'}, 200

@app.route('/load/local', methods=['POST'])
def handle_load_request_local():
    data = request.get_json()
    RD_CRM_TOKEN = data.get('RD_CRM_TOKEN')
    BQ_PROJECT_ID = data.get('BQ_PROJECT_ID')
    BQ_DATASET = data.get('BQ_DATASET')
    BQ_CREDENTIALS = data.get('BQ_CREDENTIALS')
    # Credentials
    bq_client = bq_service_account_auth(credentials=BQ_CREDENTIALS)
    rd_client = RDClient(RD_CRM_TOKEN)
    load_all(rd_client=rd_client, bq_client=bq_client, BQ_PROJECT_ID=BQ_PROJECT_ID, BQ_DATASET=BQ_DATASET)
    return {'message': 'Job executed successfully'}, 200

@app.route('/update_deals', methods=['POST'])
def handle_upd_request():
    data = request.get_json()
    RD_CRM_TOKEN = data.get('RD_CRM_TOKEN')
    pipeline_id = data.get('pipeline_id')
    deals_table_id = data.get('deals_table_id')
    prods_table_id = data.get('prods_table_id')
    deals = data.get('deals', True)
    products = data.get('products', False)
    deals = False if deals=="False" or deals=="false" else True
    products = True if products=="True" or products=="true" else False
    # Credentials
    bq_client = bigquery.Client()
    rd_client = RDClient(RD_CRM_TOKEN)
    update_deals(
        rd_client=rd_client,
        bq_client=bq_client,
        pipeline_id=pipeline_id,
        deals_table_id=deals_table_id,
        prods_table_id=prods_table_id,
        deals=deals,
        products=products
    )
    return {'message': 'Job executed successfully'}, 200

@app.route('/update_deals/local', methods=['POST'])
def handle_upd_request_local():
    data = request.get_json()
    RD_CRM_TOKEN = data.get('RD_CRM_TOKEN')
    BQ_CREDENTIALS = data.get('BQ_CREDENTIALS')
    pipeline_id = data.get('pipeline_id')
    deals_table_id = data.get('deals_table_id')
    prods_table_id = data.get('prods_table_id')
    deals = data.get('deals', True)
    products = data.get('products', False)
    deals = False if deals=="False" or deals=="false" else True
    products = True if products=="True" or products=="true" else False
    # Credentials
    bq_client = bq_service_account_auth(credentials=BQ_CREDENTIALS)
    rd_client = RDClient(RD_CRM_TOKEN)
    update_deals(
        rd_client=rd_client,
        bq_client=bq_client,
        pipeline_id=pipeline_id,
        deals_table_id=deals_table_id,
        prods_table_id=prods_table_id,
        deals=deals,
        products=products
    )
    return {'message': 'Job executed successfully'}, 200

# For local tests the app is executed directly by that script
if __name__=='__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))