import json

with open("data/athena_table_fields.json", "r") as read_file:
    json_data = json.load(read_file)

list = json_data['table']
for i in list:
    # print(i['Name'] + ' ' + i['Type'] + ',' )
    print("`{0}` {1},".format(i['Name'], i['Type']))


# partition_s3_key = "datekey=2023-04-01/hour=12/"
#
# prcd_datekey_est = partition_s3_key