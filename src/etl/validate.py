from airflow.exceptions import AirflowException
from airflow.models import Variable
from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import DataContextConfig, InMemoryStoreBackendDefaults
from datetime import datetime

import great_expectations as gx

ACCESS_KEY = Variable.get('acces_key')
SECRET_KEY = Variable.get('secret_key')
ENDPOINT = "minio:4666"
BUCKET= Variable.get('bucket')

def gx_config(root_dir:str=None) -> gx.DataContext:

  if root_dir is None:
    project_config = DataContextConfig(
      store_backend_defaults=InMemoryStoreBackendDefaults()
    )
    context = EphemeralDataContext(project_config=project_config)
  else:
    # data_context_config = DataContextConfig(
    #   data_docs_sites={
    #       "s3_site": {
    #           "class_name": "SiteBuilder",
    #           "store_backend": {
    #               "class_name": "TupleS3StoreBackend",
    #               "bucket": BUCKET,
    #               "aws_access_key_id": ACCESS_KEY,
    #               "aws_secret_access_key": SECRET_KEY,
    #               "endpoint_url":f"http://{ENDPOINT}"
    #           },
    #           "site_index_builder": {
    #               "class_name": "DefaultSiteIndexBuilder"
    #           }
    #       }
    #   }
    # )

    context = gx.get_context(context_root_dir=root_dir)
    # context._project_config["data_docs_sites"] = data_context_config["data_docs_sites"]

  return context

def gx_validator(context:gx.DataContext, asset_name:str, source_name:str, suite_name:str) -> tuple:


  datasource = context.sources.add_or_update_spark_s3(
      name='minio_source'
      ,bucket=BUCKET
      ,boto3_options={ 
          "aws_access_key_id": ACCESS_KEY,
          "aws_secret_access_key": SECRET_KEY,
          "endpoint_url":f"http://{ENDPOINT}"
        }
    )

  asset = datasource.add_parquet_asset(name=asset_name,
                                      batching_regex=r'incidents.parquet'
                                    , s3_prefix=f'refined/{datetime.now().strftime("%Y%m%d")}'
                                    , s3_recursive_file_discovery=True)
  
  runtime_batch_request = asset.build_batch_request()

  # df_source = context.sources.add_or_update_spark(name=asset_name)
  # df_assert = df_source.add_dataframe_asset(name=source_name, dataframe=df)
  # runtime_batch_request = df_assert.build_batch_request()

  context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

  validator = context.get_validator(
    batch_request=runtime_batch_request,
    expectation_suite_name=suite_name,
  )

  return validator, runtime_batch_request

def gx_checkpoint(runtime_batch_request:gx.core.batch.RuntimeBatchRequest ,context:gx.DataContext, checkpoint_name:str, suite_name:str) -> gx.checkpoint.types.checkpoint_result.CheckpointResult:

  checkpoint = Checkpoint(
      name=checkpoint_name,
      run_name_template=f"%Y%m%d-%H%M%S-{checkpoint_name}-template",
      data_context=context,
      batch_request=runtime_batch_request,
      expectation_suite_name=suite_name,
      action_list=[
          {
              "name": "store_validation_result",
              "action": {"class_name": "StoreValidationResultAction"},
          },
          {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"}},
          {
            "name":"send_email_on_validation_result",
            "action":{
              "class_name":"EmailAction",
              "renderer": {
                "module_name": "great_expectations.render.renderer.email_renderer",
                "class_name":"EmailRenderer"
                },
              "notify_on":"all",
              "use_ssl": True,
              "use_tls": False,
              "smtp_address": "smtp.gmail.com",
              "smtp_port": 587,
              "sender_login": "joaoallmeida96@gmail.com",
              "sender_password": "jzskjrrqatkopjrf",
              "sender_alias": "joaoallmeida96@gmail.com",
              "receiver_emails":"jaumalmeida@outlook.com"            
              }
          }
      ],
  )

  context.add_or_update_checkpoint(checkpoint=checkpoint)

  checkpoint_result = checkpoint.run()

  return checkpoint_result


def test_suite(df_validator:gx.validator.validator.Validator) -> gx.validator.validator.Validator:

  orderColumns = ["traffic_report_id","published_date","issue_reported","latitude", "longitude","address", "traffic_report_status", "traffic_report_status_date_time", "year", "month"]

  # Check Null columns
  nullColumns = ['address','traffic_report_id','traffic_report_status','latitude','longitude']

  # Check Date types
  longColumns = ['latitude','longitude']
  stringColumns = ['address','issue_reported','traffic_report_status']
  timestampColumns = ['published_date','traffic_report_status_date_time']

  df_validator.expect_table_columns_to_match_set(column_set=orderColumns,exact_match=True)

  for col in longColumns:
    df_validator.expect_column_values_to_be_of_type(column=col,  type_="FloatType")

  for col in stringColumns:
    df_validator.expect_column_values_to_be_of_type(column=col, type_="StringType")

  for col in timestampColumns:
    df_validator.expect_column_values_to_be_of_type(column=col, type_="TimestampType")

  for col in nullColumns:
    df_validator.expect_column_values_to_not_be_null(column=col)

  df_validator.save_expectation_suite(discard_failed_expectations=False)

  return df_validator

############################################

context_root_dir = "./great_expectations/"
asset_name='traffic_incident_dataframe'
source_name='spark_datasource'
suite_name="expectation_suite_traffic_incident"
site_name="traffic_incident_site"
checkpoint_name="my-checkpoint"

context = gx_config(context_root_dir)
validator, runtime_batch_request = gx_validator( context, asset_name, source_name, suite_name)

df_validator = test_suite(validator)
checkpoint_result = gx_checkpoint( runtime_batch_request, context, checkpoint_name, suite_name)

context.build_data_docs(site_names=site_name)

validation_result = list(checkpoint_result.run_results.items())[0][1]["validation_result"]

if not validation_result['success']:
  failed_result = [a for a in validation_result['results'] if a['success'] is not True]
  print(failed_result)
  raise AirflowException('Validation of the source data loading is not successful')