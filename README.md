# multifamily_pricing

To run the pipeline, call the `run_pricing_pipeline` function within `main.py`

To deploy a pipeline for automatic runs on new uploads to the associated gcs bucket, run the command below (you will need to authenticate to gcp first and have the appropriate permissions, as well as have created the bucket to be listened to)

```console
gcloud functions deploy python-finalize-function \
--runtime=python38 \
--region=us-west1 \
--source=multifamily_pricing \
--entry-point=run_pricing_pipeline \
--trigger-bucket="coysu-demo-datasets" \
--memory=1024MB
``` 

Request #1:

We can see through the standardized validation process that simply joining the census data as additional features significantly improves performance (at least anecdotally, we would need to bootstrap the process to measure statistical significance.) It is also worth noting that no feature engineering work was done here, and that would be an obvious place to begin to invest in incremental performance.

[Experiment without census data](https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/demo-pipeline-20220913171148?project=demos-362417)

{"training_performance": 3.5521078523339176, "test_performance": 5.779388753911955}

[Experiment with census data](https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/demo-pipeline-20220914122822?project=demos-362417)

[Relevant git branch](https://github.com/secrettoad/multifamily_pricing/tree/experiment-add-census-data)

{"training_performance": 2.2145700468597287, "test_performance": 5.218189891420213}

Request #2:

Monotonicity with respect to square footage is already implemented in XGBoost, and so we implement it within the `train_component` function.

Request #3:

The `Google Cloud Functions` cli command provided at the beginning of this document creates a serverless, asynchronous function that listens for new files being added to the relevant bucket. The pipeline then uses `dask` to load all relevant files within each train and test directory.

Request #4:

Within the function `deploy_component` a model is pushed to the `gcp` model registry and then exposed via an http endpoint. Due to a severe time constraint, comparing version performance was left out of scope.

To answer the question, why `Kubeflow` on `Vertex AI`: It is simply the fastest way to implement the requested work in a commercial-grade, production-ready manner. I will demonstrate the relevant `Vertex AI` functionality during the presentation.



