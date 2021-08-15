# udacity_data_engineering_nanodegree__project_3


# Setup

## Config file
- Make a copy of the file `dwh.cfg.sample` and call it just `dwh.cfg`.
- Enter your credentials. Make sure not to use any quotation marks.

## Dependencies
- Install the pipenv virtual environment by running `pip3 install pipenv`.
- Set up the virtual environment by navigating to the root folder
and running `pipenv install`.
  
## How to start it
The project is still a work in progress. So far it creates the
Redshift cluster and its necessary AWS resources. I'm working on getting
the DB connection.

To try out the functionality mentioned above, navigate to
`aws_resources.py` and comment out/uncomment the sections to run.
Then run `pipenv run python3 aws_resources.py`.