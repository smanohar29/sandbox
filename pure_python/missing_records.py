# import collections
#
# test = "[[09, 1.1395489970161655], [10, 5.1641653777329643], [12, 2.1238078884740930], [16, 4.1145850143881664]]"
# test = test.replace('[', '')
# test = test.replace(']', '')
# test = test.replace(',', '')
#
# y = list(test.split())
#
# print(y)
#
# hour = [y[0], y[2], y[4], y[6]]
# # print('hour: ' + str(hour))
#
# # score = [float(y[1]), float([3]), float(y[5]), float(y[7])]
# # print('score: ' + str(score))
#
# print(type(y[0]))
# print(y[0])
#
# # res = dict(zip(hour,score))
# # # print("dict: " + str(res))
# #
# # sorted_x = sorted(res.items(), key=lambda kv: kv[1], reverse=1)
# # sorted_dict = collections.OrderedDict(sorted_x)
# #
# # result = list(sorted_dict.keys())
# #
# # print(sorted_x)
# # print(result)
# # print(result[0])
#

import  re

# new = "loannumber|    campaignname|   campaignstartdate|campaignenddate| h_sloan|addressstatus|h_twebcredit|h_thome_search_status|h_thearaboutuscd|            h_tgoals|h_lisva|h_tisservicing|h_scurrrate|typeoflead|h_twoupb|h_trealestateagent|h_tsalesprice|h_tupemclassic|h_tleasingmonth|h_tfirsttimehomebuyer|h_tlbfirsttimehomebuyer|h_tzip|h_toffer|upemmodeltype|h_tleasingyear|h_ttotmthlydebt|h_teveareaphone|h_tyearhomebought|     h_tcity|h_tleasing|h_tupemgrade|h_tcurrentdate|h_tdownpayment|h_tcontactinfuture|       correlationId|h_sselfcreditrating|h_tcurrbal|h_tbaseloanamt|h_tcashout|h_tppresentval|h_sstate|            h_temail|h_tleadtype|h_tvenleadid|       h_tadproperty|h_tdupsystemname|hasclientrefinancedbefore|h_tconfirmationemail|currentloanvendor|h_tprptytypcd|h_torigmorttype|      leadcreatedate|ltc_close_rate|state_close_rate|adproperty_parsed|adproperty_close_rate|escalationcount|callingclientdatetime|preesc_previoustransfercount|preesc_previouscallcount|timezone|leadcreatehour|clientemaildomain|clientemailtopleveldomain|leadtimebucket|localleadcreatehour|localleadtimebucket|leadagedays|          createdate|  leadcreatedatetime|createdtid|loanpurpose|leadtypebucket|earlymorningflag|morningflag|lunchtimeflag|afternoonflag|eveningflag|nightflag|localearlymorningflag|localmorningflag|locallunchtimeflag|localafternoonflag|localeveningflag|localnightflag|dateid|calldateid|previousdaycalldateid|marketing_partner|previouscallscount|earlymorningcallcount|morningcallcount|lunchtimecallcount|afternooncallcount|eveningallcount|nightcallcount|localearlymorningcallcount|localmorningcallcount|locallunchtimecallcount|localafternooncallcount|localeveningallcount|localnightcallcount|callcount|previousdaycallcount|currentdaycallcount|calldayofweek|weekendflg|holidayflg|hourofdial|dialtimebucket|localhourofdial|localdialtimebucket|localcalltstarttime|     insertdatetime|campaign_group|   datekey|       hash|       prediction|      scoring_ts_utc|      scoring_ts_est|modelversion"
# test = re.sub('\s+', '', new).strip()
# # test = test.replace("`", "")
# test_list_1 = list(test.split('|'))
# print(test_list_1)
# print("length 1: " + str(len(test_list_1)) )
#
# old = "loannumber, modelversion, prediction, h_tlbfirsttimehomebuyer, h_twoupb, h_tadproperty, h_tzip, h_sstate, h_tcity, h_ttotmthlydebt, h_tppresentval, h_tcashout, h_tbaseloanamt, h_tyearhomebought, h_tisservicing, h_scurrrate, h_tcurrbal, h_tsalesprice, h_trealestateagent, h_lisva, h_sselfcreditrating, h_tprptytypcd, h_tcontactinfuture, addressstatus, typeoflead, h_temail, h_sloan, h_teveareaphone, h_tfirsttimehomebuyer, h_tgoals, h_thearaboutuscd, h_tupemclassic, h_tleasingmonth, h_tleasingyear, h_tleasing, h_toffer, h_tdownpayment, h_thome_search_status, h_twebcredit, h_torigmorttype, h_tleadtype, h_tconfirmationemail, h_tcurrentdate, h_tupemgrade, upemmodeltype, h_tvenleadid, leadcreatedate, callingclientdatetime, timezone, leadcreatehour, clientemaildomain, clientemailtopleveldomain, leadtimebucket, localleadcreatehour, leadagedays, createdate, leadcreatedatetime, createdtid, loanpurpose, leadtypebucket, earlymorningflag, morningflag, lunchtimeflag, afternoonflag, eveningflag, nightflag, localearlymorningflag, localmorningflag, locallunchtimeflag, localafternoonflag, localeveningflag, localnightflag, dateid, calldateid, previousdaycalldateid, previouscallscount, earlymorningcallcount, morningcallcount, lunchtimecallcount, afternooncallcount, eveningallcount, nightcallcount, localearlymorningcallcount, localmorningcallcount, locallunchtimecallcount, localafternooncallcount, localeveningallcount, localnightcallcount, previousdaycallcount, currentdaycallcount, calldayofweek, weekendflg, holidayflg, hourofdial, dialtimebucket, localhourofdial, localdialtimebucket, insertdatetime"
# test = re.sub('\s+', '', old).strip()
# test_list_2 = list(test.split(','))
# print(test_list_2)
# print("length 2: " + str(len(test_list_2)) )
#
#
# set1 = set(test_list_1)
# set2 = set(test_list_2)
#
# print('\n')
#
# missing = list(set1 - set2)
# print('missing field(s) in maestro_features_scoring_current_1_4: ' + str(missing))
#
# missing = list(set2 - set1)
# print('missing field(s) in dp: ' + str(missing))

# for i in range(0,len(test_list_1)):
#     if test_list_1[i] not in test_list_2:
#         # print(str(test_list_1[i]) + " not present")
#         print(str(test_list_1[i]))

# sample_list = list()
#
# i = set2.intersection(set1)
#
# count = 0
#
# for i in test_list_2:
#     if i in test_list_1:
#         print(i)
#         count = count+1
#         sample_list.append(i)
#
# # print(count)
# print(sample_list)


# test = '["loannumber" ,"campaignname" ,"campaignstartdate" ,"campaignenddate" ,"h_sloan" ,"addressstatus" ,"h_twebcredit" ,"h_thome_search_status" ,"h_thearaboutuscd" ,"h_tgoals" ,"h_lisva" ,"h_tisservicing" ,"h_scurrrate" ,"typeoflead" ,"h_twoupb" ,"h_trealestateagent" ,"h_tsalesprice" ,"h_tupemclassic" ,"h_tleasingmonth" ,"h_tfirsttimehomebuyer" ,"h_tlbfirsttimehomebuyer" ,"h_tzip" ,"h_toffer" ,"upemmodeltype" ,"h_tleasingyear" ,"h_ttotmthlydebt" ,"h_teveareaphone" ,"h_tyearhomebought" ,"h_tcity" ,"h_tleasing" ,"h_tupemgrade" ,"h_tcurrentdate" ,"h_tdownpayment" ,"h_tcontactinfuture" ,"correlationid" ,"h_sselfcreditrating" ,"h_tcurrbal" ,"h_tbaseloanamt" ,"h_tcashout" ,"h_tppresentval" ,"h_sstate" ,"h_temail" ,"h_tleadtype" ,"h_tvenleadid" ,"h_tadproperty" ,"h_tdupsystemname" ,"hasclientrefinancedbefore" ,"h_tconfirmationemail" ,"currentloanvendor" ,"h_tprptytypcd" ,"h_torigmorttype" ,"leadcreatedate" ,"ltc_close_rate" ,"state_close_rate" ,"adproperty_parsed" ,"adproperty_close_rate" ,"escalationcount" ,"callingclientdatetime" ,"preesc_previoustransfercount" ,"preesc_previouscallcount" ,"timezone" ,"leadcreatehour" ,"clientemaildomain" ,"clientemailtopleveldomain" ,"leadtimebucket" ,"localleadcreatehour" ,"localleadtimebucket" ,"leadagedays" ,"createdate" ,"leadcreatedatetime" ,"createdtid" ,"loanpurpose" ,"leadtypebucket" ,"earlymorningflag" ,"morningflag" ,"lunchtimeflag" ,"afternoonflag" ,"eveningflag" ,"nightflag" ,"localearlymorningflag" ,"localmorningflag" ,"locallunchtimeflag" ,"localafternoonflag" ,"localeveningflag" ,"localnightflag" ,"dateid" ,"calldateid" ,"previousdaycalldateid" ,"marketing_partner" ,"previouscallscount" ,"earlymorningcallcount" ,"morningcallcount" ,"lunchtimecallcount" ,"afternooncallcount" ,"eveningallcount" ,"nightcallcount" ,"localearlymorningcallcount" ,"localmorningcallcount" ,"locallunchtimecallcount" ,"localafternooncallcount" ,"localeveningallcount" ,"localnightcallcount" ,"callcount" ,"previousdaycallcount" ,"currentdaycallcount" ,"calldayofweek" ,"weekendflg" ,"holidayflg" ,"hourofdial" ,"dialtimebucket" ,"localhourofdial" ,"localdialtimebucket" ,"localcalltstarttime" ,"insertdatetime" ,"datekey" ,"campaign_group"]'
# test = re.sub('\s+', '', test).strip()
# print(test)


