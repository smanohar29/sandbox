# import pyspark.sql.functions as F
#
# # t = "2019-10-15 17:18:23"
# #
# # res = t.rsplit(" ")[0]
# # res = res.replace('-', '')
# # print(res)
#
# from decimal import Decimal
# from fractions import Fraction
# import json
# import simplejson as sjson
#
# score1 = '3.728784700674E-5'
# score2 = '3.728784700674E-5'
# score3 = '0.32588101602236'
#
#
# print(float(score1))
# print(Decimal(score2))
# print(Decimal(score3))
#
# # print(sjson.dumps(Decimal(score3)))

# print(int(hashlib.sha1(lno.encode('utf-8')).hexdigest(), 16) % (10 ** 8))
# print(int(hashlib.sha1(lno.encode('utf-8')).hexdigest(), 16) % (10 ** 19))

# splitConfig= {'groups': [{'label': 'Leadbuy_Purchase', 'majorVersion': 1, 'minorVersion': 0, 'iteration': 1, 'testingGroup': 'LeadQuality_A', 'qname': 'lead_quality_leadbuy_purchase_rsp', 'pre_processing_config': '1_0_1_lead_quality_lead_buy_purchase.json', 'coefficient_config': 'lead_buy_purchase_1_0_1.json', 'fraction': '0.95', 'leadgrade_config': ''}, {'label': 'Random', 'majorVersion': 1, 'minorVersion': 0, 'iteration': 1, 'testingGroup': 'Random', 'qname': 'lead_quality_leadbuy_purchase_rsp', 'pre_processing_config': 'Random.json', 'coefficient_config': 'Random.json', 'fraction': '0.05', 'leadgrade_config': ''}]}
#
# totalGroups = len(splitConfig['groups'])
# # print(totalGroups)
#
# label = []
# fraction = []
#
# for i in range(0, totalGroups):
#     print(i)
#     # label[i] = splitConfig['groups'][i]['label']
#     # fraction[i] = splitConfig['groups'][i]['fraction']
#
# print('label: ' + str(label))
# print('fraction: ' + str(fraction))

epoch=0
save_path = 'hdfs://datalakeprod/uat/DataScienceSandbox/Maestro/ContactMethod/model/model_{}.h5'
model_path = save_path.format(epoch)
print(" MODEL SAVED {} - ".format(model_path))









