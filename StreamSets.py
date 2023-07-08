from streamsets.sdk import ControlHub
sch = ControlHub(credential_id='24a3ec99-5d86-4745-a607-e987835deae7', token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiOTBjYjY5YWFjODI5NjVjMGMyYjFkYWJkOTFhMjJjMjY1N2M4NDYwNDFmYmFmODNmMzU4M2UwYTI1MDI2M2VlYTVjNWY2Mzc4NWY2N2ExZTY1NjIwYzkyNTk3YjNhOWUwOTYxZThjY2M0ZTVmYTY1ZWYyZTNlNzdhN2RjNzM3YmEiLCJ2IjoxLCJpc3MiOiJldTAxIiwianRpIjoiMjRhM2VjOTktNWQ4Ni00NzQ1LWE2MDctZTk4NzgzNWRlYWU3IiwibyI6ImRjZTVhZjZiLWUzMGUtMTFlYy05NWU3LTNkZjAzODVlMDViZiJ9.')
if sch is not None:
    print("connection successfull",sch)

builder = sch.get_pipeline_builder(engine_type='data_collector',engine_id='0f93a683-1d26-4a35-906e-b4402bc2c85e')

dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
trash = builder.add_stage('Trash')
dev_raw_data_source >> trash
pipeline = builder.build('My first pipeline from SDK')