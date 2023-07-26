# test_string = "hello how are you ? how many times have you eaten today ? are you saying saying this is a bad time to eat food ! I hope you are wrong becuase I do not agree with you . I hope you have a good day "
#
# test = test_string.split(' ')
#
# dictOfWords = {}
#
# for word in test:
#     if word not in dictOfWords:
#         dictOfWords[word] = 0
#     dictOfWords[word] += 1
#
# sorted_dict = sorted(dictOfWords.items(), key=lambda x: x[1] , reverse=True)
# print(sorted_dict)
#
# # counter=0
# #
# # for i in  sorted_dict:
# #     counter += 1
# #     if counter>5:
# #         break
# #     else:
# #         print(i)
#
# print('\n')
#
# from collections import Counter
# print(Counter(test_string.split()).most_common(5))

from datetime import date, timedelta, datetime

# d1 = date(2019, 1, 1)  # start date
# d2 = date(2019, 1, 5)  # end date
#
# delta = d2 - d1
# print(delta.days)
#
# for i in range(delta.days + 1):
#     receiveddate = d1 + timedelta(i)
#     print(receiveddate)

start_timestamp = "2020-06-01 08:30:00.000"
end_timestamp = "2020-06-02 23:30:00.000"

start_timestamp = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
end_timestamp = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')

while start_timestamp <= end_timestamp:

    if str(start_timestamp).split()[1] not in ('00:30:00', '01:30:00', '02:30:00', '03:30:00', '04:30:00', '05:30:00', '06:30:00', '07:30:00'):
        # print(start_timestamp)

        dt = "'" + str(start_timestamp).split()[0] + "'"
        hr = "'" + str(start_timestamp).split()[1].split(':')[0] + "'"

        query = "SELECT * FROM model_offline_feature_store_uat.lead_allocation_features_loans_1_0_0_historical_fix WHERE campaign_group=='defib' and datekey={0} and hourofdial={1}"
        print(query.format(dt ,hr))

    start_timestamp = start_timestamp + timedelta(hours=1)



















