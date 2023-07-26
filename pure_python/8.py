# record = {
#                        "Request": {
#                           "TimeStamp": "09/10/2019 00:00:01.852-04:00",
#                           "Key": "LeadAugmentedEvent",
#                           "MessageSource": "SubmissionEngine",
#                           "Payload": {
#                              "LoanNumber": 12346,
#                              "CorrelationId": "9bd68aee-4f17-4d7d-a6d6-9d0dcc9e0eed",
#                              "H_tLeadType": "ltSOMETHING",
#                              "H_tadProperty": "",
#                              "H_tVenLeadId": 123,
#                              "H_sLoan": "purchase",
#                              "H_tStreet": "123 wings st",
#                              "H_tFname": "Steve",
#                              "H_tLname": "Yzerman",
#                              "H_tEmail": "SteveYzerman@gmail.com",
#                              "H_sState": "MI",
#                              "H_tZip": "48201",
#                              "H_tEveAreaPhone": "313",
#                              "H_tDupSystemName": "null",
#                              "H_tIsServicing": False,
#                              "H_lisVA": "no",
#                              "H_tPrptyTypCd": "null",
#                              "H_tContactInFuture": "False",
#                              "AddressStatus": "Approved",
#                              "TypeOfLead": "Website",
#                              "H_tOrigMortType": "null",
#                              "H_tpPresentVal": "null",
#                              "H_tCashOut": "null",
#                              "H_tBaseLoanAmt": 65000,
#                              "H_sCurrRate": "null",
#                              "H_tCurrBal": 65000
#                           }
#                        }
#                     }
#
#
# for i in range(1,3):
#     # record['Request']['Payload']['LoanNumber'] + i*5
#     record['Request']['Payload']['LoanNumber'] = i
#     print(record)
#
#
# # for k, val in record.items():
# #     print(k)
# #     print(val)
# #
# #     my_dict = {**record, 'key1': 'value1', 'key2': 'value2'}



# import ast
#
# def txt_to_dict(path):
#    """This takes a text file and returns a dictionary. It helps read in config files.
#    Args:
#        path (str): path to .txt file structured like a dictionary
#    Returns:
#        (dict) from the passed text files
#    """
#    file = open(path, "r")
#    txt = file.read()
#    final = ast.literal_eval(txt)
#    return final
#
# models = {"LBP": {},
#               "WR": {}}
# print(models)
#
# # models = {"LBP": {()},
# #               "WR": {{}}}
#
# models['LBP']['config'] = txt_to_dict("app/config/preprocessing/LeadBuy_Purchase/preprocess_1_0_0.txt")
#
#
# print(models)

import numpy as np
val = 0.9

if val in np.arange(0.012758, 1):
   print("valid: " + str(val))

