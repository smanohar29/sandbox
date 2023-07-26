# import json
#
# with open("data/BDIA_Alerting_Sample.json", "r") as read_file:
#     config = json.load(read_file)
#
# # if "count_alert" in config:
# #     print("count_alert present")
# #
# # if "partition_alert" in config:
# #     print("partition_alert present")
#
# if config['count_alert']['targetQuery'] in ('' or ' '):
#     print('targetQuery is empty')
#
# # print(config)
#
# # print(config['count_alert']['targetQuery'])


partitionColumns = ["datekey", "hour"]
print(partitionColumns)

