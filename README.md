# RD CRM to BigQuery Pipeline

This application is an easy-to-use (and deploy) solution to populate a BigQuery dataset with a set of tables from RD Station CRM data. It will provide the data necessary to analyse the CRM structure and performance from the account.

The application and code can be used in some different ways. Mainly, you can import data for exploration and configure ETL processes to feed BI dashboards for everyday analysis. This README file will describe how to deploy the application as a Google Cloud Run service, that can be used to get data from multiple CRM accounts, but you can use the code locally or deploy the app in different contexts.

If you encounter any bugs or opportunities to improve, you can contact me and/or create an issue so this project can keep evolving. Also, the code was written to be shared and used at will, so feel free to copy, refine and modify it to your own needs. 

## Features

- Retrieve data from RD CRM using the RD CRM API
In the rd module, the RDClient class authenticates access when instantiated, and each method fetches data from an endpoint and retrieves it in different suitable ways (mainly pandas DataFrames);

- Organizes and loads the data in BigQuery
The app and jobs provide different options for creating and updating specific tables.

## Prerequisites

- Access to an RD CRM account [token](https://ajuda.rdstation.com/s/article/Gerar-e-visualizar-Token?language=en_US), go in the link to get help on how to get it;
- A Google Cloud project, and service account with access to Google BigQuery and the required permissions to upload data. The BigQuery authentication can be obtained by a [service account credentials .json file](https://cloud.google.com/iam/docs/service-account-overview) or is done by default when using the service account to execute jobs in the deployed application;
- Make sure to have all the Google Cloud API's that you'll use enabled;
- If you don't intend to use the application built by the Dockerfile, make sure to use a virtual python environment and install all the requirements from the requirements.txt file.

## Instructions

You will need to create a Google Cloud Run service from that repository by following the steps below

## Creating a Cloud Run service
<!-- Criar passo a passo para criação de serviço Cloud Run -->

### Jobs variables
The variables needed to execute each job need to be in a post request payload and they can vary for different job configurations. 
<!-- Criar arquivo .md com referência de endpoints -->
Here follows the descriptions to each variable:

- RD_CRM_TOKEN: The RD CRM account authorization token;
- BQ_PROJECT_ID: Your Googloe Cloud project ID;
- BQ_DATASET: The BigQuery dataset ID where you want the CRM data tables to be writen (the dataset needs to be created before code execution for it to work);
- BQ_CREDENTIALS: A dictionary with the content of the service account credentials .json file.

## Data Tables

The data loaded in Big Query should be suffice to provide a wide range of CRM data analysis variety, but you will probably need to treat the data in BQ for your specific purpose. Here follows a list of data tables that should be loaded in your dataset:

- pipelines: Deal pipelines;
- custom_fields: Custom fields;
- stages: Pipeline stages set for all deal pipelines;
- sources: All deal sources;
- products: All products;
- teams: Teams of user set;
- users: Users set;
- deal_lost_reasons: Deal lost reasons;
- campaigns: Campaigns;
- Multiple deals tables, one for each pipeline, limited to 10.000 rows for each pipeline as the RD CRM API limits (you can make workarounds on that if needed).

The specific fields and custom fields that will be displayed depend on specific account configuration and data. As RD CRM is highly versatile, I tried to set it in a way that I would get what I need in most cases.

If you have any problems, sugestions or alternative solutions for these kinds of scenarios, please reach me on e-mail (joaoaufsousa@gmail.com) or [LinkedIn](www.linkedin.com/in/joão-sousa-11488a19a).