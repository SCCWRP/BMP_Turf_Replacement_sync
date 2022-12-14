import boto3, os, json
import pandas as pd
from sqlalchemy import create_engine
from decimal import Decimal
from boto3.dynamodb.conditions import Key

eng = create_engine(os.environ.get('DB_CONNECTION_STRING'))

#(tbl_watervolume_final."timestamp")::character varying AS "timestamp_chr",
qry = """
    SELECT
        CONCAT(
            CAST(tbl_watervolume_final.sitename AS VARCHAR), '_', 
            CAST(tbl_watervolume_final.sensorgroup AS VARCHAR),'_',
            CAST(tbl_watervolume_final.sitelocation AS VARCHAR)
        ) AS site_grp_sensor,
        CAST(extract(epoch FROM "timestamp") AS DECIMAL) AS "timestamp_numeric",
        CAST(tbl_watervolume_final.sitename AS VARCHAR) AS sitename,
        CAST(tbl_watervolume_final.sensorgroup AS VARCHAR) AS sensorgroup,
        CAST(tbl_watervolume_final.sitelocation AS VARCHAR) AS sitelocation,
        CAST("timestamp" AS VARCHAR) AS "timestamp",
        CAST(tbl_watervolume_final.wvc_final AS DECIMAL) AS result,
        CAST(tbl_watervolume_final.wvcunit AS VARCHAR) AS unit
    FROM tbl_watervolume_final
    WHERE tbl_watervolume_final.wvc_final IS NOT NULL
"""

print ("Query the watervolume table")
tmpdf = pd.read_sql(qry, eng)
tmpjson = tmpdf.to_dict('records')

dynjsonstr = '\n'.join([ json.dumps({"Item": {k: {"S" if type(v) == str else "N": f"{v}"}  for k,v in x.items() } }) for x in tmpjson ])

print("dynjsonstr")
print(dynjsonstr)

jsonpath = 'download/turfdata.json'

with open(jsonpath, 'w') as f:
    f.write(dynjsonstr)


# AWS DynamoDB requires the numbers to be Decimals
# print('AWS DynamoDB requires the numbers to be Decimals')
# tmpjson = json.loads(json.dumps(tmpjson), parse_float=Decimal)

# access our resource. Authentication is handled behind the scenes with environment variables, (access keys/tokens etc.)
# AWS_SECRET_ACCESS_KEY = .....
# AWS_ACCESS_KEY_ID = .....
# AWS_DEFAULT_REGION = .....
# dyn_resource = boto3.resource('dynamodb')
# dyn_client = boto3.client('dynamodb')
# table = dyn_resource.Table('sd_turf_sensor_data')
# print("write data to AWS S3 Bucket")


# # Test the query
# print('# Test the query speed')
# qryresult = dyn_client.query(
#     TableName = 'sd_turf_sensor_data',
#     IndexName="sitename-timestamp_numeric-index", 
#     ExpressionAttributeValues = {
#         ':s' : {
#             'S': 'Fieldcrest'
#         },
#         ':ts1': {
#             'N': '1645168680'
#         },
#         ':ts2': {
#             'N': '1645168800'
#         },
#     },
#     KeyConditionExpression = 'sitename = :s AND (timestamp_numeric BETWEEN :ts1 AND :ts2)'
# )

# print("qryresult")
# print(qryresult)