from datetime import datetime

start = datetime.now()

end  = datetime.now()

timeTaken = end - start
print(timeTaken)



# #
# # test = '03/24/2014 12:00:50.927-04:00'
# #
# # dt = datetime.strptime(test, "%m/%d/%Y %H:%M:%S.%f%z")
# # # print(str(dt))
# #
# # x = test.rsplit()
# data = {}
# data['h_temail'] = "test@gmail.com"
# # data['clientemaildomain'] = '@'+data['h_temail'].split("@", 1)[-1]
# test = data['h_temail'].split("@", 1)
# print(test)
# print(test[0])
# print(test[1])
#
#
# test_json = {
#
# {
#     "projectName": "Dialing Priority Strategy Deployment",
#     "scoreSplit": {
#         "hashColumn": "loannumber",
#         "bins": [
#             {
#                 "fraction": "0.475",
#                 "label": "DP_A_1_4_5_2",
#                 "majorVersion": 1,
#                 "minorVersion": 4,
#                 "iteration": 5,
#                 "excludeFlag": 1,
#                 "preprocessConfig": "Dialing_Priority_1_4_8.json"
#             },
#             {
#                 "fraction": "0.475",
#                 "label": "DP_B_2_0_0",
#                 "majorVersion": 1,
#                 "minorVersion": 4,
#                 "iteration": 5,
#                 "excludeFlag": 0,
#                 "preprocessConfig": "Dialing_Priority_2_0_0.json"
#             },
#             {
#                 "fraction": "0.05",
#                 "label": "RandomControl",
#                 "excludeFlag": 0
#             }
#         ]
#     }
# }
#
# }
# import json
#
# with open("data/Dialing_Priority_2_0_0.json", "r") as read_file:
#     config = json.load(read_file)
#
# t = config['preprocess']
#
# for col in t['fill_median']:
#     print(col)
#
# print(config['preprocess']['fill_median'])