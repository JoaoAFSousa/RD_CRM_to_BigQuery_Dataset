# RD CRM to BigQuery Pipeline

This repository contains a Python script that acts as a pipeline to fetch data from RD CRM and upload it to Google BigQuery. The purpose is to provide an easy-to-use solution for getting CRM data and using it for analysis or dashboard building from a BigQuery dataset.

## Features

- Retrieve data from RD CRM using the RD CRM API.
- Transform the data into a format suitable for Google BigQuery.
- Upload the data to BigQuery for easy analysis and reporting.

## Prerequisites

Before running the script, make sure you have the following:

- **Python 3.x** (preferably 3.9 or higher);
- Access to an RD CRM account [token](https://ajuda.rdstation.com/s/article/Gerar-e-visualizar-Token?language=en_US), go in the link to get help on how to get it;
- A Google Cloud project with access to Google BigQuery and the required permissions to upload data, I you to do the authentication by a [service account credentials .json file](https://cloud.google.com/iam/docs/service-account-overview), as the script is already set to do that (instructions below). 
- Make sure you have all the python libraries installed and versioned as the requirements.txt file describes.

## Instructions

You will need to insert API credentials information on the python script code as the following instructions describe.

### Google Cloud Service Account credentials
To make the authentication you will need to clone this repository and insert your service account .json credentials file inside it. Once you have done that, write the credentials file relative path inside the python script as indicated.

### Environment variables
For simplicity, the environment variables need to be set inside the python script. So you will need to write them where the code comments indicate. Follows the list of environment variables you will need to set:

- RD_CRM_TOKEN: The RD CRM account authorization token;
- BQ_PROJECT_ID: Your Googloe Cloud projectr ID;
- BQ_DATASET: The BigQuery dataset ID where you want the CRM data tables to be writen (The dataset need to be created before code execution for it to work)

Now you can run the python script and a set of tables should be writen in a few minutes. Depending on how much leads and pipelines you have in your CRM account it will take more or less time.

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

For updating the tables, if you run the code again, it overwrites all the tables, using the dataframe schemas, so you should have no problem if there are deleted or new custom fields, pipelines and so on.

As a requests library begginer, I developed this script as a quick solution for accessing multiple CRM data in my work, and I hope it works for other people purposes as well. If you have any problems, sugestions or alternative solutions for these kinds of scenarios, feel free to reach me by e-mail on:

joaoaufsousa@gmail.com