export AWS_ACCESS_KEY_ID=ASIAXNMMPAKNHVWP36OB
export AWS_SECRET_ACCESS_KEY=POVUfqt8l0UUntN9pq2g2PWpViAWHc7ri1xrCPZV
export AWS_SESSION_TOKEN=FwoGZXIvYXdzEPD//////////wEaDAxRd1HgA5TrWCthYyKsATN0AhvlFZ//mFxxBdvW7V4rXVuow/hW/HIbOmhBkOAKNbOiqdEs2guymAIrlAWppBQ26B/QH6mpv19aKW0yvnjyzzVOjRqK59ee0E1hQpj3C7S8Krc8ftEVOVK5kkVl92bUIgM3R7LBZBbLTsdi2guW37KKA/dj7sOFCy3dolvwMY7HoeMay6IHRGlhoH1t7Hf7q4AObYDPdArCJ7ZIcKAfghD2CdxysKZnqaAope7MhQYyLRaI2mkdyHPlXUGHE70F7YUQcDFItvcNYinCT/4j1ga10ucVSsYDdNnOxzh4kg==

cd dsc_102
chmod 400 featureprep_ec2.pem
ssh -i featureprep_ec2.pem ubuntu@ec2-18-237-212-25.us-west-2.compute.amazonaws.com -L 8001:localhost:8787

sudo apt-get update
sudo apt-get install awscli
sudo apt-get install python3-venv
python3 -m venv env
source env/bin/activate

pip install dask[complete] --no-cache-dir
aws s3 cp s3://ds102-mintchoco-scratch/data/historical_data_2009Q1/historical_data_2009Q1.txt ~/
python
