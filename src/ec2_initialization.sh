# HOW TO SSH INTO EC2 INSTANCE ----------------------------------------------------------------------------------------------------
#cd dsc_102 # [OPTIONAL] change directories to the folder that has the pem file on siddhi's laptop
#chmod 400 featureprep_ec2.pem # change permissions in this file to be correct
#ssh -i featureprep_ec2.pem ubuntu@ec2-34-222-214-216.us-west-2.compute.amazonaws.com -L 8001:localhost:8787 # ssh into EC2 instance

# WHAT TO DO ONCE IN THE EC2 INSTANCE ---------------------------------------------------------------------------------------------
sudo apt-get update # update your repositories
sudo apt-get install awscli # upload aws command line interface

# export the API keys onto the instance
export AWS_ACCESS_KEY_ID=ASIAXNMMPAKNHVWP36OB 
export AWS_SECRET_ACCESS_KEY=POVUfqt8l0UUntN9pq2g2PWpViAWHc7ri1xrCPZV
export AWS_SESSION_TOKEN=FwoGZXIvYXdzEPD//////////wEaDAxRd1HgA5TrWCthYyKsATN0AhvlFZ//mFxxBdvW7V4rXVuow/hW/HIbOmhBkOAKNbOiqdEs2guymAIrlAWppBQ26B/QH6mpv19aKW0yvnjyzzVOjRqK59ee0E1hQpj3C7S8Krc8ftEVOVK5kkVl92bUIgM3R7LBZBbLTsdi2guW37KKA/dj7sOFCy3dolvwMY7HoeMay6IHRGlhoH1t7Hf7q4AObYDPdArCJ7ZIcKAfghD2CdxysKZnqaAope7MhQYyLRaI2mkdyHPlXUGHE70F7YUQcDFItvcNYinCT/4j1ga10ucVSsYDdNnOxzh4kg==

sudo apt-get install python3-venv # download python virtual env package
python3 -m venv env # activate venv
source env/bin/activate # activates this virtual environment as our default python environment

pip install dask[complete] --no-cache-dir # install dask
aws s3 cp s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_2009Q1.txt ~/ # copy the file you want to read temporarily into your local EC2 instance
# python # activate python

# add feature_prep.py file to s3 bucket
aws s3 cp s3://ds102-mintchoco-scratch/feature_prep.py ~/
pip install dask-ml
