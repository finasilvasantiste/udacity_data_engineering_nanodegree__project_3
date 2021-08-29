# udacity_data_engineering_nanodegree__project_3


# Setup

## Config file
- Make a copy of the file `dwh.cfg.sample` and call it just `dwh.cfg`.
- Replace the empty/indicated values with your credentials, and leave the filled out values as they are. 
  Make sure not to use any quotation marks.

## Dependencies
- Install the pipenv virtual environment by running `pip3 install pipenv`.
- Set up the virtual environment by navigating to the root folder
and running `pipenv install`.
- Make sure your python path is set correctly by running:
``export PYTHONPATH=$PATHONPATH:`pwd```
  
## How to run the app
- Create all aws resources needed to set up redshift by running `pipenv run python3 aws/create_aws_resource.py`.
  Running that will also populate the file `aws_role_arn.json`.
  

- Create all tables by running `pipenv run python3 create_tables.py`.


- Run the ETL process by running `pipenv run python3 etl.py`


- Delete all aws resources by running `pipenv run python3 aws/delete_aws_resource.py`
