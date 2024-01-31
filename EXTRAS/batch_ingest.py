import boto3
import json
from datetime import datetime as dt


# """Works only with Aurora serverless clusters - Must have Data API enabled"""
rds_data = boto3.client('rds-data')
firehose = boto3.client('firehose')
query = """
        SELECT * FROM dbo.streamflow_data
"""
query_response = rds_data.execute_statement(
    continueAfterTimeout=False,
    database='waterdata',
    includeResultMetadata=True,
    resourceArn='arn:aws:rds:us-west-2:459793645210:cluster:waterdata-serverless-cluster',
    sql=query,
    secretArn='arn:aws:secretsmanager:us-west-2:459793645210:secret:new-waterdata-serverless-secret-ZAxeym'
)

# peak_flow_2023 = query_response['records'][0][0]['longValue']  # Assuming it's a long type, adjust if otherwise   
# print("Peak Streamflow for 2023 = ", peak_flow_2023, " cfs")

# resultset = query_response['records']
columns = ['agency_cd', 'site_no', 'datetime', 'discharge_cfs']
#print(resultset)
record_batch = []
json_record_batch = None
# for row in resultset:
#     # print(row)
#     record_to_stream = {}
#     row_list = []
#     for d in row:
#         # print(d)
#         for k, v in d.items():
#             try:
#                 v = float(v)
#             except ValueError as e:
#                 pass
#             try:
#                 v = int(v)
#             except ValueError as e:
#                 pass
#             try:
#                 v = dt.strptime(v, '%Y-%m-%d %H:%M:%S.%f')
#             except ValueError as e:
#                 pass
#             except TypeError as e:
#                 pass
#             try:
#                 v = dt.strptime(v, '%Y-%m-%d').date()
#             except ValueError as e:
#                 pass
#             except TypeError as e:
#                 pass
#             finally:
#                 row_list.append(v)
    # record_to_stream["Data"] = bytes(str(json.dumps(dict(zip(columns, row_list)), default=str)), 'utf-8')
    # record_batch.append(record_to_stream)

print(record_batch)
batch_response = firehose.put_record_batch(
    DeliveryStreamName='kinesis-firehose-delivery-stream-demo',
    Records=record_batch
)

print(batch_response)

