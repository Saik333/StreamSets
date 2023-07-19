import os
from streamsets.sdk import ControlHub
from dotenv import load_dotenv

load_dotenv()

try:
    sch = ControlHub(credential_id=os.getenv('id'),token=os.getenv('token'))
    print("Connection to ControlHub is Successfull")
except:
    print("Connection to ControlHub is Failed")
    raise

pipeline_builder = sch.get_pipeline_builder(engine_type='data_collector', engine_id='0f93a683-1d26-4a35-906e-b4402bc2c85e')

print("adding jdbc query consumer")
jdbc_query_consumer = pipeline_builder.add_stage('JDBC Query Consumer', type='origin')

jdbc_query_consumer.set_attributes(
    jdbc_connection_string=os.getenv('sql_jdbc_url'),
    sql_query='select * from Study.tbl_staff',
    incremental_mode=False,
    ***REMOVED***=os.getenv('sql_***REMOVED***'),
    password=os.getenv('sql_password')
)

print("adding expression evaluator")
expression_eval = pipeline_builder.add_stage('Expression Evaluator', type='processor')
expression_eval.set_attributes(
    field_expressions=[
        {"fieldToSet":"/load_time","expression":"${time:now()}"},
    ]
)

print("adding snowflake")
snowflake = pipeline_builder.add_stage('Snowflake',type='destination')
snowflake.set_attributes(
    include_organization=False,
    snowflake_region='OTHER',
    custom_snowflake_region=os.getenv('snowflake_region'),
    account=os.getenv('snowflake_account'),
    user=os.getenv('snowflake_***REMOVED***'),
    password=os.getenv('snowflake_password'),
    warehouse='COMPUTE_WH',
    database='SS_TRAINING',
    schema='SS_TRAINING_PIPELINE',
    table='TEST_SDK_SQL_SNF',
    table_auto_create=True,
    data_drift_enabled=True,
    stage_location='INTERNAL',
    snowflake_stage_name='STREAMSETS_STAGE'
)

print("adding pipeline finisher")
pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

pipeline_finisher.set_attributes(
    react_to_events=True,
    event_type='no-more-data',
    reset_origin=True,
    on_record_error='DISCARD'
)

print('making connection between stages')
jdbc_query_consumer >> expression_eval
expression_eval >> snowflake
jdbc_query_consumer >= pipeline_finisher

print("creating the pipeline and publishing it")
pipeline = pipeline_builder.build("SQL_SNF_SDK")
sch.publish_pipeline(pipeline=pipeline,commit_message="my second pipeline commit from SDK")

print("Pipeline created successfully and published")


# print("creating job")

job_builder = sch.get_job_builder()
pipeline_for_job = sch.pipelines.get(name="SQL_SNF_SDK")
job = job_builder.build('JOB FOR SQL_SNF_SDK',pipeline=pipeline_for_job)

job.data_collector_labels=['streamsetstraining_dep']

print("creating job")
sch.add_job(job)

print("process is successful")
print("starting job")
sch.start_job(job)
print("job started")
