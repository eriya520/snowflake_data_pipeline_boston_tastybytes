USE ROLE accountadmin;
CREATE DATABASE tasty_repo;
USE SCHEMA public;

-- Create credentials
CREATE OR REPLACE SECRET tasty_repo.public.github_pat
  TYPE = password
  USERNAME = ''
  PASSWORD = '';

-- Create the API integration
CREATE OR REPLACE API INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('') -- URL to your GitHub profile
  ALLOWED_AUTHENTICATION_SECRETS = ()
  ENABLED = TRUE;

-- Create the git repository object
CREATE OR REPLACE GIT REPOSITORY tasty_repo.public.snowflake_pipeline_boston_weather_tastybytes
  API_INTEGRATION =  -- Name of the API integration defined above
  ORIGIN = '' -- Insert URL of forked repo
  GIT_CREDENTIALS = ;

-- List the git repositories
SHOW GIT REPOSITORIES;