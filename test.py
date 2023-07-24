# Import the ControlHub class from the SDK.
from streamsets.sdk import ControlHub

# Connect to the StreamSets DataOps Platform.
sch = ControlHub(credential_id='2c7125b1-9bfb-4d46-8674-ec74b5da10ee', token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiYWQxYWQwZDY3ZGQ0NzE2ODU4YjdkZTYyMzQ5MTRiYWIzZTYzMjBhOTkwOTVlYTQ4ZTQ3ZTI5MWIxNjlhMjBlOWVkNWE3MjA4MTJhMDg1MDQwMGUxN2MyNDYxOGVkMTA3MjJmOWYxNDI4MjE2ZjFjODQ2YzhlZDA3OGE3NGRmZWIiLCJ2IjoxLCJpc3MiOiJldTAxIiwianRpIjoiMmM3MTI1YjEtOWJmYi00ZDQ2LTg2NzQtZWM3NGI1ZGExMGVlIiwibyI6ImRjZTVhZjZiLWUzMGUtMTFlYy05NWU3LTNkZjAzODVlMDViZiJ9.')

# Instantiate an EnvironmentBuilder instance to build an environment, and activate it.
environment_builder = sch.get_environment_builder(environment_type='SELF')
environment = environment_builder.build(environment_name='Sample Environment from SDK3',
                                        environment_type='SELF',
                                        environment_tags=['self-managed-tag'],
                                        allow_nightly_engine_builds=False)
# Add the environment and activate it
sch.add_environment(environment)
sch.activate_environment(environment)

# Instantiate the DeploymentBuilder instance to build the deployment
deployment_builder = sch.get_deployment_builder(deployment_type='SELF')

# Build the deployment and specify the Sample Environment created previously.
deployment = deployment_builder.build(deployment_name='Sample Deployment DC-DOCKER from SDK3',
                                      deployment_type='SELF',
                                      environment=environment,
                                      engine_type='DC',
                                      engine_version='5.4.0',
                                      deployment_tags=['self-managed-tag'])
deployment.install_type = 'DOCKER'
deployment.engine_instances = 1

# Add the deployment to SteamSets DataOps Platform, and start it
sch.add_deployment(deployment)
sch.start_deployment(deployment)