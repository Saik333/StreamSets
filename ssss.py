from streamsets.sdk import ControlHub

try:
    sch = ControlHub(credential_id='bcebbf32-021b-444d-9c0a-b09584b9f270sgsggsg',token='eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJzIjoiN2E0NWNhZTViNDcyN2ZjMjlkODY2MTdkZGZmMjY2NmNjMGNhOTNlNTMwNmMwNmQzMTJkNmRlNzJlNGIyMTU2ZTBlODg1OGU5YjE1NzMwZDJiYTcxZWQ1MzNmZTdiMjkzZDQ5NWEyNTExNjUxNjlkOTA0Y2E1MDI1MGE2NGJlNzYiLCJ2IjoxLCJpc3MiOiJldTAxIiwianRpIjoiYmNlYmJmMzItMDIxYi00NDRkLTljMGEtYjA5NTg0YjlmMjcwIiwibyI6ImRjZTVhZjZiLWUzMGUtMTFlYy05NWU3LTNkZjAzODVlMDViZiJ9.')
    print("Connection to ControlHub is Successfull")
except:
    print("Connection to ControlHub is Failed")
    raise