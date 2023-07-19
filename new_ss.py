import logging
from streamsets.sdk import ControlHub
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(message)s]",
    datefmt="%Y-%M-%d %HH:%MM:%SS"
)
logging.info('starting the process')
try:
    sch = ControlHub(credential_id='99a0bc56-e1ee-4dda-a455-fc9469fdfc56', token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiM2ZjYzc2MmViN2I1YTM3ZDg1YTk2ZjU0NWJmMWNhMTE2MTgyYjI0OTBmYzQxMTJmMWJmMTExMDBjYmM0N2NkOTcwMGY5OWQzZGVhMjVlNDY4MWY2YjgyY2UxMTNhNjc5MmRiMmFiZTM1Zjg2ZmNlNDkzMTI3OGE4ODQwMmQ2NjIiLCJ2IjoxLCJpc3MiOiJldTAxIiwianRpIjoiOTlhMGJjNTYtZTFlZS00ZGRhLWE0NTUtZmM5NDY5ZmRmYzU2IiwibyI6ImRjZTVhZjZiLWUzMGUtMTFlYy05NWU3LTNkZjAzODVlMDViZiJ9.')
    logging.info("Connection to controlHub is Successful")
except Exception as e:
    logging.info("Connection failed")
    raise

# sdc = sch.data_collectors.get(url='https://eu01.hub.streamsets.com/sch/jobRunner/engines/dataCollectors/0f93a683-1d26-4a35-906e-b4402bc2c85e')
pipeline_builder = sch.get_pipeline_builder(engine_type='data_collector', engine_id='0f93a683-1d26-4a35-906e-b4402bc2c85e')

jdbc_query=pipeline_builder.add_stage('JDBC Query Consumer',type='origin')
# print(dir(jdbc_query))
jdbc_query.set_attributes(
    jdbc_connection_string='jdbc:sqlserver://soch-poc.database.windows.net:1433;databaseName=SochData',
    incremental_mode=False,
    sql_query="select * from Study.tbl_staff",
    ***REMOVED***="SochPoc",
    password="India@1290",
)

expression_evaluator=pipeline_builder.add_stage('Expression Evaluator', type='processor')

expression_evaluator.set_attributes(
    field_expressions=[
        {"fieldToSet":"/load_time","expression":"${time:now()}"}]
)

snowflake=pipeline_builder.add_stage('Snowflake',type='destination')
# print(dir(snowflake))
snowflake.set_attributes(
    include_organization=False,
    snowflake_region='OTHER',
    custom_snowflake_region="central-india.azure",
    account="dk89728",
    user="saikumar",
    password="Sai@12345",
    warehouse="COMPUTE_WH",
    database="SS_TRAINING",
    schema="SS_TRAINING_PIPELINE",
    table="SDK_TEST_TABLE",
    table_auto_create=True,
    stage_location='INTERNAL',
    snowflake_stage_name='STREAMSETS_STAGE'
)

pipeline_finisher=pipeline_builder.add_stage('Pipeline Finisher Executor')
pipeline_finisher.set_attributes(
    reset_origin=True,
    react_to_events=True,
    event_type='no-more-data',
    on_record_error="DISCARD",
)

# print(dir(pipeline_finisher))

jdbc_query >> expression_evaluator >> snowflake
jdbc_query >= pipeline_finisher

pipeline=pipeline_builder.build('test_pipeline_11')
sch.publish_pipeline(pipeline=pipeline,commit_message="My first pipeline from SDK")