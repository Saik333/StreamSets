from streamsets.sdk import ControlHub

try:
    sch = ControlHub(credential_id='bcebbf32-021b-444d-9c0a-b09584b9f270',token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiN2E0NWNhZTViNDcyN2ZjMjlkODY2MTdkZGZmMjY2NmNjMGNhOTNlNTMwNmMwNmQzMTJkNmRlNzJlNGIyMTU2ZTBlODg1OGU5YjE1NzMwZDJiYTcxZWQ1MzNmZTdiMjkzZDQ5NWEyNTExNjUxNjlkOTA0Y2E1MDI1MGE2NGJlNzYiLCJ2IjoxLCJpc3MiOiJldTAxIiwianRpIjoiYmNlYmJmMzItMDIxYi00NDRkLTljMGEtYjA5NTg0YjlmMjcwIiwibyI6ImRjZTVhZjZiLWUzMGUtMTFlYy05NWU3LTNkZjAzODVlMDViZiJ9.')
    print("Connection to ControlHub is Successfull")
except:
    print("Connection to ControlHub is Failed")
    raise

pipeline_builder = sch.get_pipeline_builder(engine_type='data_collector', engine_id='0f93a683-1d26-4a35-906e-b4402bc2c85e')

dev_raw_data = pipeline_builder.add_stage('Dev Raw Data Source')

# print(dir(dev_raw_data))
print("Adding Dev Raw Data Source")
dev_raw_data.set_attributes(
    label="Generating test data",
    raw_data="""{
  "f1": "abc",
  "f2": "xyz",
  "f3": "lmn",
  "f4": "abcd"
}""",
    stop_after_first_batch=True
)

print("Adding Trash Stage")

trash = pipeline_builder.add_stage('Trash')
trash.set_attributes(
    label="Loading into Trash Destination"
)

print("connecting the stages")
dev_raw_data >> trash

print("creating the pipeline and publishing it")
pipeline = pipeline_builder.build("FIRST_SDK_TEST_PIPELINE")
sch.publish_pipeline(pipeline=pipeline,commit_message="my first commit from SDK")

print("Pipeline created successfully and published")


# print("creating job")

job_builder = sch.get_job_builder()
pipeline_for_job = sch.pipelines.get(name="FIRST_SDK_TEST_PIPELINE")
job = job_builder.build('JOB FOR FIRST_SDK_TEST_PIPELINE',pipeline=pipeline_for_job)

job.data_collector_labels=['streamsetstraining_dep']

print("creating job")
sch.add_job(job)

print("process is successful")




#pip3 install streamsets~=5.1.0