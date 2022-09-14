from google.cloud import aiplatform
from kfp import dsl
from kfp.v2 import compiler
from kfp.v2.dsl import component, Input, Output, Dataset, Model, Artifact
import functions_framework
import tempfile


@component(packages_to_install=['dask[dataframe]', 'pyarrow', 'gcsfs'], base_image='python:3.7')
def ingest_component(df_uri: str, census_df_uri: str, df: Output[Dataset]):
    import dask.dataframe as dd
    import datetime
    _df_census = dd.read_csv(census_df_uri).set_index('Unnamed: 0')
    _df = dd.read_csv(df_uri).set_index('Unnamed: 0')
    _df['year'] = (datetime.datetime(year=2014, month=12, day=30) + dd.to_timedelta(_df['days_since_2014'], unit='days')).dt.year
    _df_merge = _df.merge(_df_census, on=['blockgroup', 'year'])
    _df_merge.to_parquet(df.uri)


@component(packages_to_install=['dask[dataframe]', 'xgboost', 'scikit-learn', 'pyarrow', 'gcsfs'], base_image='python:3.7')
def train_component(df_train: Input[Dataset], model: Output[Model], params: Output[Artifact]):
    import xgboost as xgb
    import dask.dataframe as dd
    import gcsfs
    import pickle
    import json

    fs = gcsfs.GCSFileSystem()
    _df_train = dd.read_parquet(df_train.uri).compute()
    X = _df_train[['latitude', 'longitude', 'property_type', 'sqft', 'beds', 'baths', 'days_since_2014']]
    y = _df_train['trans_log_price']
    ##TODO add capability for distributed cluster on compute engine
    ##TODO add hyperparameter tuning
    ##TODO invert control of parameters
    _params = {"params": {"objective": "reg:squarederror", "random_state": 111, "monotonic_constraints": {"sqft": 1}}}
    _model = xgb.XGBRegressor(**_params['params'])
    _model.fit(X, y)
    model.uri = model.uri + '.pkl'

    with fs.open(model.uri, 'wb') as f:
        pickle.dump(_model, f)

    with fs.open(params.uri, 'w') as f:
        json.dump(_params, f)


@component(packages_to_install=['dask[dataframe]', 'xgboost', 'scikit-learn', 'pyarrow', 'gcsfs'], base_image='python:3.7')
def predict_component(df_predict: Input[Dataset], model: Input[Model], y_hat: Output[Dataset]):
    import dask.dataframe as dd
    import pickle
    import gcsfs

    fs = gcsfs.GCSFileSystem()

    _df_predict = dd.read_parquet(df_predict.uri).compute()

    X = _df_predict[['latitude','longitude','property_type','sqft','beds','baths','days_since_2014']]

    with fs.open(model.uri, 'rb') as f:
        _model = pickle.load(f)

    _df_predict['y_hat'] = _model.predict(X)

    dd.from_pandas(_df_predict, chunksize=1000000).to_parquet(y_hat.uri)


@component(packages_to_install=['dask[dataframe]', 'pyarrow', 'gcsfs',], base_image='python:3.7')
def validate_component(y_hat: Input[Dataset], y_hat_test: Input[Dataset], metrics: Output[Artifact]):
    import dask.dataframe as dd
    import gcsfs
    import json
    import numpy as np

    fs = gcsfs.GCSFileSystem()

    _y_hat = dd.read_parquet(y_hat.uri).compute()
    _y_hat_test = dd.read_parquet(y_hat_test.uri).compute()

    def inv_zscore_log_price(y, mean, std):
        return np.exp((y * std) + mean)

    def median_absolute_percentage_error(actual, predicted):
        return np.median((np.abs(actual - predicted) / actual)) * 100

    def moments(s):
        return s.apply(np.log).mean(), s.apply(np.log).std()


    _metrics = {}
    _metrics['training_performance'] = median_absolute_percentage_error(inv_zscore_log_price(_y_hat['y_hat'], *moments(_y_hat['price'])),
                                           inv_zscore_log_price(_y_hat['trans_log_price'], *moments(_y_hat['price'])))
    _metrics['test_performance'] = median_absolute_percentage_error(inv_zscore_log_price(_y_hat_test['y_hat'], *moments(_y_hat['price'])),
                                           inv_zscore_log_price(_y_hat_test['trans_log_price'], *moments(_y_hat['price'])))

    with fs.open(metrics.uri, 'w') as f:
        json.dump(_metrics, f)


@component(packages_to_install=['dask[dataframe]', 'pyarrow', 'gcsfs', 'google-cloud-aiplatform'], base_image='python:3.7')
def deployment_component(model: Input[Model], metrics: Input[Artifact], vertex_endpoint: Output[Artifact], vertex_model: Output[Model]):
    from google.cloud import aiplatform
    import gcsfs
    import json
    aiplatform.init(project='demos-362417')

    fs = gcsfs.GCSFileSystem()
    with fs.open(metrics.uri, 'rb') as f:
        _metrics = json.load(f)

    ##TODO compare against current version performance
    ##TODO log performance metrics to metadata
    if _metrics['test_performance'] > 0:

        _model = aiplatform.Model.upload(
            display_name="multifamily_demo_model",
            artifact_uri='/'.join(model.uri.split('/')[:-1]),
            serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
        )

        endpoint = _model.deploy(machine_type="n1-standard-8")
        vertex_endpoint.uri = endpoint.resource_name
        vertex_model.uri = _model.resource_name


@dsl.pipeline(
    name="demo-pipeline",
    description="demo",
    pipeline_root='gs://coysu-demo-pipelines/multifamily-pricing',
)
def pipeline():
    train_data = ingest_component(df_uri='gs://coysu-demo-datasets/multifamily_pricing/train/*.csv', census_df_uri='gs://coysu-demo-datasets/multifamily_pricing/census/*.csv')
    test_data = ingest_component(df_uri='gs://coysu-demo-datasets/multifamily_pricing/test/*.csv', census_df_uri='gs://coysu-demo-datasets/multifamily_pricing/census/*.csv')
    model = train_component(train_data.outputs['df'])
    insample_preds = predict_component(train_data.outputs['df'], model.outputs['model'])
    test_preds = predict_component(test_data.outputs['df'], model.outputs['model'])
    metrics = validate_component(insample_preds.outputs['y_hat'], test_preds.outputs['y_hat'])
    deployed_model = deployment_component(model.outputs['model'], metrics.outputs['metrics'])

    ###todo start here - make serving pipeline and add business logic - add splitting to train pipeline


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def run_pricing_pipeline(event=None):
    template_path = tempfile.gettempdir() + '/pipeline.json'
    compiler.Compiler().compile(pipeline_func=pipeline, package_path=template_path)

    aiplatform.init(project='demos-362417', staging_bucket="gs://coysu-demo-pipelines/multifamily_pricing/staging")

    job = aiplatform.PipelineJob(
        display_name='multifamily_demo',
        template_path=template_path,
        pipeline_root='gs://coysu-demo-pipelines/multifamily_pricing'
    )

    job.run(sync=False)