# A Snowflake data engineering project-- Food truck sale trends and weather metrics in Boston

## Brief introduction
This project showcases how to use snowflake, Snow CLI, and Git CI/CD workflow to achieve ingesting raw data from AWS S3 and transform the raw data to show the sale trends and weather metrics from Boston, USA in a streamlit APP. The aim is to explore the insights of daily sales and weather metrics via an interactive dashboard. 
In addition, we will create streaming table and manually insert new order data to test the streaming functions, create email notification for sales that are below a certain criteria.
The data engineering project follows a CI/CD work flow. We create two branches in the repo: staging as for developing and testing and prod as for production and delivery. 
The API connection and git configuration are done behind the scene but are necessary to allow snowsight and git to work.

Languages: SQL, Python
Platforms: AWS, Snowflake, Snowpipe, Snow CLI, GitHub

## Approach
### Goal 1: Visualize sales trends and weather metrics
* Ingest data from S3 via staging in Snow CLI
* Create two branches for CI/CD workflow: `STAGING` and `PROD`
* Creat views for data transformation and test it in `FEATURE BRANCH` under `STAGING`
* Build a streamlit app and visualize the sales trends and weather metrics of Boston by year
* In th GitHub Repo, set up an auto testing workflow in Github Actions
* Review and test the code, once satisfied, create pull and merge request
* Merge to `PROD` once the `STAGING` branch pass all test.

### Goal 2: Streaming table and email notification
* create a streaming table and test it by manually insert new order data; 
* send email nofitification for any new sales that has missing sales information;
