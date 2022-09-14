# multifamily_pricing

gcloud functions deploy python-finalize-function \
--runtime=python38 \
--region=us-west1 \
--source=multifamily_pricing \
--entry-point=run_pricing_pipeline \
--trigger-bucket="coysu-demo-datasets" \
--memory=1024MB