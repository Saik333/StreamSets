import logging
from streamsets.sdk import ControlHub
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(message)s]",
    datefmt="%Y-%M-%d %HH:%MM:%SS"
)
logging.info('starting the process')
try:
    sch = ControlHub(credential_id='2c7125b1-9bfb-4d46-8674-ec74b5da10ee', token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiYWQxYWQwZDY3ZGQ0NzE2ODU4YjdkZTYyMzQ5MTRiYWIzZTYzMjBhOTkwOTVlYTQ4ZTQ3ZTI5MWIxNjlhMjBlOWVkNWE3MjA4MTJhMDg1MDQwMGUxN2MyNDYxOGVkMTA3MjJmOWYxNDI4MjE2ZjFjODQ2YzhlZDA3OGE3NGRmZWIiLCJ2IjoxLCJpc3MiOiJldTAxIiwianRpIjoiMmM3MTI1YjEtOWJmYi00ZDQ2LTg2NzQtZWM3NGI1ZGExMGVlIiwibyI6ImRjZTVhZjZiLWUzMGUtMTFlYy05NWU3LTNkZjAzODVlMDViZiJ9.')
    logging.info("Connection to controlHub is Successful")
except Exception as e:
    logging.info("Connection failed")
    raise

# sdc = sch.data_collectors.get(url='https://eu01.hub.streamsets.com/sch/jobRunner/engines/dataCollectors/0f93a683-1d26-4a35-906e-b4402bc2c85e')
pipeline_builder = sch.get_pipeline_builder(engine_type='data_collector', engine_id='0f93a683-1d26-4a35-906e-b4402bc2c85e')
dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
logging.info("adding stage dev_raw_data_source")
dev_raw_data_source.set_attributes(
    stop_after_first_batch=False,
    raw_data=r"""{
  "f1": "abc",
  "f2": "xyz",
  "f3": "lmn",
  "f4":"opq"
}"""
)
# print(dir(dev_raw_data_source))
logging.info("adding stage trash")
trash = pipeline_builder.add_stage('Trash')
dev_raw_data_source >> trash

#create pipeline
pipeline = pipeline_builder.build('SDK_TEST_11')
logging.info("publishing new pipeline")
sch.publish_pipeline(pipeline, commit_message='SDK_TEST_11')

#create job
logging.info("creating job for the pipeline")
job_builder = sch.get_job_builder()
pipeline = sch.pipelines.get(name='SDK_TEST_11')
job = job_builder.build('job_My first pipeline from SDK TEST11', pipeline=pipeline, runtime_parameters={'name':"NA"})
job.data_collector_labels=['streamsetstraining_dep']
sch.add_job(job)
logging.info("Process executed successfully")
sch.start_job(job)