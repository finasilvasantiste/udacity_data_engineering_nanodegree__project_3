# udacity_data_engineering_nanodegree__project_3


# Setup

## Config file
- Make a copy of the file `dwh.cfg.sample` and call it just `dwh.cfg`.
- Enter your credentials. Make sure not to use any quotation marks.

## Dependencies
- Install the pipenv virtual environment by running `pip3 install pipenv`.
- Set up the virtual environment by navigating to the root folder
and running `pipenv install`.
- Make sure your python path is set correctly by running:
``export PYTHONPATH=$PATHONPATH:`pwd```
  
## How to start it
- Create all aws redshift resources by running `pipenv run python3 aws/create_aws_resource.py`
- Delete all aws redshift resources by running `pipenv run python3 aws/delete_aws_resource.py`
