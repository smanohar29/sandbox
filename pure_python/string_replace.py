
## Convert field list from pandas data frame to spark data frame

import re

# test = ''' attributes_entityid|loannumber|sourcereferenceid|state|  loanpurpose|iscurrentqlclient|isqlms|monthlycitycountylientaxamount|monthlyhoiamount|dwelling_type|clientincomeannualamount|subjectpropertytype|subjectpropertyoccupancytype|isinvestmentproperty|subjectpropertyliencurrentbalance|subjectpropertylienremainingtermmonths|subjectpropertylienmonthlypayment|subjectpropertylienoriginalbalance|subjectpropertylienoriginaltermmonths|subjectpropertylienoriginalagencytype|subjectpropertypropertyvalue|finalcreditscore|nonmortgagerevolvingdebt|nonmortgagemonthlydebt|    state_close_rate|prediction|leadgrade|   datekey|  '''
# test = re.sub('\s+', '', test).strip()
# test =test.lower()
# # test =test.replace("|", "` string, \n")
# test = test.replace("|", '", "')
# print(test)
#
# #
# # #TODO Uncomment to run
# # # for i in test.split(','):
# # #     print ('StructField("'+ str(i) + '", StringType(),True)')

## Convert field list to create hive table defintion

# fields = """    dateid
#                     ,loannumber
#                     ,h_tlbfirsttimehomebuyer
#                     ,h_twoupb
#                     ,h_tadproperty
#                     ,h_tzip
#                     ,h_sstate
#                     ,h_tcity
#                     ,h_ttotmthlydebt
#                     ,h_tppresentval
#                     ,h_tcashout
#                     ,h_tbaseloanamt
#                     ,h_tyearhomebought
#                     ,h_tisservicing
#                     ,h_scurrrate
#                     ,h_tcurrbal
#                     ,h_tsalesprice
#                     ,h_trealestateagent
#                     ,h_lisva
#                     ,h_sselfcreditrating
#                     ,h_tprptytypcd
#                     ,h_tcontactinfuture
#                     ,addressstatus
#                     ,typeoflead
#                     ,h_temail
#                     ,h_sloan
#                     ,h_teveareaphone
#                     ,h_tfirsttimehomebuyer
#                     ,h_tgoals
#                     ,h_thearaboutuscd
#                     ,h_tupemclassic
#                     ,h_tleasingmonth
#                     ,h_tleasingyear
#                     ,h_tleasing
#                     ,h_toffer
#                     ,h_tdownpayment
#                     ,h_thome_search_status
#                     ,h_twebcredit
#                     ,h_torigmorttype
#                     ,h_tleadtype
#                     ,h_tconfirmationemail
#                     ,h_tcurrentdate
#                     ,h_tupemgrade
#                     ,upemmodeltype
#                     ,h_tvenleadid
#                     ,correlationid
#                     ,h_tfname
#                     ,h_tlname
#                     ,h_tb1nickname
#                     ,h_tb2fname
#                     ,h_tb2lname
#                     ,h_tb2nickname
#                     ,h_tb2email
#                     ,h_tstreet
#                     ,h_tb2street
#                     ,h_tb2city
#                     ,h_tb2state
#                     ,h_tb2zip
#                     ,h_tmthlyincome
#                     ,h_tb2mthlyincome
#                     ,canshowproofofincome
#                     ,h_tisdeduped
#                     ,h_temployeryears
#                     ,h_temployername
#                     ,h_tselfemployed
#                     ,h_tb2employeryears
#                     ,h_tb2employername
#                     ,h_tselfemployed2
#                     ,employmentstatus
#                     ,h_tispropertyowner
#                     ,h_tb2ispropertyowner
#                     ,h_tisleasing
#                     ,h_trentpayment
#                     ,h_townrent
#                     ,h_tdesiredpayment
#                     ,h_tmopi
#                     ,h_tauthcode
#                     ,contactbyrealestateagent
#                     ,agentid
#                     ,corporatedirect
#                     ,hasclientrefinancedbefore
#                     ,h_tinitintrrate
#                     ,h_tprevcashoutamt
#                     ,h_tprevrefipurpose
#                     ,currentloanvendor
#                     ,ratetype
#                     ,cohortproduct
#                     ,desiredratetype
#                     ,h_tpurchaseagreement
#                     ,h_tlbsignedpurchaseagreement
#                     ,h_tleadcost
#                     ,leadduration
#                     ,h_ssource
#                     ,leadsourcecategorycode
#                     ,sourceid
#                     ,subsource
#                     ,ishighpriority
#                     ,issweepstakes
#                     ,upemversion
#                     ,h_tupemcurrent
#                     ,h_tpromotioncode
#                     ,h_thasforeclosure
#                     ,h_thasbankruptcy
#                     ,h_tb2hasforeclosure
#                     ,h_tb2hasbankrutpcy
#                     ,h_cnosol
#                     ,h_tprimaryresidence
#                     ,h_tb2primaryresidence
#                     ,propertyuse
#                     ,h_ttimeframetobuy
#                     ,h_tlbtimeframetobuy
#                     ,foundnewhome
#                     ,referredbycommonid
#                     ,h_tcommonid
#                     ,h_trefersid
#                     ,testtag
#                     ,h_tnetbenltv
#                     ,h_tnetbenbaseamount
#                     ,h_tisavsvalidated
#                     ,h_tbday3
#                     ,h_tb2day3
#                     ,h_tmarital
#                     ,h_tbmarital
#                     ,h_tcurrpmt
#                     ,h_tdownpaymentborrowed
#                     ,numberoflatepayments
#                     ,isdisasterarea
#                     ,disasterdescription
#                     ,callcenter
#                     ,totalhops
#                     ,purchaseintentcode
#                     ,leadcreatedate
#                     ,h_tdupsystemname
#                     ,h_tcellphone
#                     ,h_tadditionalphone1
#                     ,h_tadditionalphone2
#                     ,h_tdayareaphone
#                     ,h_tdayprefixphone
#                     ,h_tdaylast4phone
#                     ,h_teveprefixphone
#                     ,h_tevelast4phone
#                     ,h_tb2cellphone
#                     ,h_tb2dayareaphone
#                     ,h_tb2dayprefixphone
#                     ,h_tb2daylast4phone
#                     ,h_tb2eveareaphone
#                     ,h_tb2eveprefixphone
#                     ,receiveddate
#                     ,LoanPoolHistoryFactID
#                     ,PoolStartDate
#                     ,WebCodeID
#                     ,LoanPoolID
#                     ,PoolStartDateID
#                     ,PoolStartTimeID
#                     ,PoolEndDate
#                     ,PoolEndDateID
#                     ,PoolEndTimeID
#                     ,EndDateDW
#                     ,CurrentFlgDW
#                     ,InsertDtDW
#                     ,LastUpdateDtDW
#                     ,LastUpdateByDW
#                     ,LTC_close_rate
#                     ,state_close_rate
#                     ,adproperty_parsed
#                     ,adproperty_close_rate
#                     ,escalationcount
#                     ,campaignname
#                     ,campaignstartdate
#                     ,campaignenddate
#                     ,campaign_group
#                     ,preesc_previoustransfercount
#                     ,preesc_previouscallcount
#                     ,callingclientdatetime
#                     ,blendeddialer_transferflag
#                     ,timezone
#                     ,leadcreatehour
#                     ,clientemaildomain
#                     ,clientemailtopleveldomain
#                     ,leadtimebucket
#                     ,localleadcreatehour
#                     ,localleadtimebucket
#                     ,leadagedays
#                     ,createdate
#                     ,leadcreatedatetime
#                     ,createdtid
#                     ,loanpurpose
#                     ,leadtypebucket
#                     ,hourofdial
#                     ,localhourofdial
#                     ,dialtimebucket
#                     ,localdialtimebucket
#                     ,earlymorningflag
#                     ,morningflag
#                     ,lunchtimeflag
#                     ,afternoonflag
#                     ,eveningflag
#                     ,nightflag
#                     ,localearlymorningflag
#                     ,localmorningflag
#                     ,locallunchtimeflag
#                     ,localafternoonflag
#                     ,localeveningflag
#                     ,localnightflag
#                     ,calldateid
#                     ,previousdaycalldateid
#                     ,marketing_partner
#                     ,previouscallscount
#                     ,earlymorningcallcount
#                     ,morningcallcount
#                     ,lunchtimecallcount
#                     ,afternooncallcount
#                     ,eveningallcount
#                     ,nightcallcount
#                     ,localearlymorningcallcount     """
#
# fields = re.sub('\s+', '', fields).strip()
# # fields =fields.replace(",", '" ,"')
# fieldList = fields.split(',')
#
# tableList = ["loannumber" ,"campaignname" ,"campaignstartdate" ,"campaignenddate" ,"h_sloan" ,"addressstatus" ,"h_twebcredit" ,"h_thome_search_status" ,"h_thearaboutuscd" ,"h_tgoals" ,"h_lisva" ,"h_tisservicing" ,"h_scurrrate" ,"typeoflead" ,"h_twoupb" ,"h_trealestateagent" ,"h_tsalesprice" ,"h_tupemclassic" ,"h_tleasingmonth" ,"h_tfirsttimehomebuyer" ,"h_tlbfirsttimehomebuyer" ,"h_tzip" ,"h_toffer" ,"upemmodeltype" ,"h_tleasingyear" ,"h_ttotmthlydebt" ,"h_teveareaphone" ,"h_tyearhomebought" ,"h_tcity" ,"h_tleasing" ,"h_tupemgrade" ,"h_tcurrentdate" ,"h_tdownpayment" ,"h_tcontactinfuture" ,"correlationid" ,"h_sselfcreditrating" ,"h_tcurrbal" ,"h_tbaseloanamt" ,"h_tcashout" ,"h_tppresentval" ,"h_sstate" ,"h_temail" ,"h_tleadtype" ,"h_tvenleadid" ,"h_tadproperty" ,"h_tdupsystemname" ,"hasclientrefinancedbefore" ,"h_tconfirmationemail" ,"currentloanvendor" ,"h_tprptytypcd" ,"h_torigmorttype" ,"leadcreatedate" ,"ltc_close_rate" ,"state_close_rate" ,"adproperty_parsed" ,"adproperty_close_rate" ,"escalationcount" ,"callingclientdatetime" ,"preesc_previoustransfercount" ,"preesc_previouscallcount" ,"timezone" ,"leadcreatehour" ,"clientemaildomain" ,"clientemailtopleveldomain" ,"leadtimebucket" ,"localleadcreatehour" ,"localleadtimebucket" ,"leadagedays" ,"createdate" ,"leadcreatedatetime" ,"createdtid" ,"loanpurpose" ,"leadtypebucket" ,"earlymorningflag" ,"morningflag" ,"lunchtimeflag" ,"afternoonflag" ,"eveningflag" ,"nightflag" ,"localearlymorningflag" ,"localmorningflag" ,"locallunchtimeflag" ,"localafternoonflag" ,"localeveningflag" ,"localnightflag" ,"dateid" ,"calldateid" ,"previousdaycalldateid" ,"marketing_partner" ,"previouscallscount" ,"earlymorningcallcount" ,"morningcallcount" ,"lunchtimecallcount" ,"afternooncallcount" ,"eveningallcount" ,"nightcallcount" ,"localearlymorningcallcount" ,"localmorningcallcount" ,"locallunchtimecallcount" ,"localafternooncallcount" ,"localeveningallcount" ,"localnightcallcount" ,"callcount" ,"previousdaycallcount" ,"currentdaycallcount" ,"calldayofweek" ,"weekendflg" ,"holidayflg" ,"hourofdial" ,"dialtimebucket" ,"localhourofdial" ,"localdialtimebucket" ,"localcalltstarttime" ,"insertdatetime" ,"datekey" ,"opportunity_thekeyopportunityid" ,"opportunity_producername" ,"opportunity_eventtimestamp" ,"opportunity_loanpurpose" ,"opportunity_opportunitygroup" ,"opportunity_opportunitytype" ,"opportunity_language" ,"opportunity_borrowers" ,"opportunity_property" ,"opportunity_eventsubtype" ,"includeinkeyflag" ,"campaign_group"]
#
# set1 = set(fieldList)
# set2 = set(tableList)
#
# missing = list(set2 - set1)
# print(missing)
#
# for i in missing:
#     if i.startswith('opportunity'):
#         continue
#     else:
#         print(i)
#
#
# # print(fields)
# # for i in fields.split(','):
# #     print (str(i) + ' string,')
#
#
# from datetime import datetime, timedelta, date
#
# # query = "SELECT * FROM leadallocation_conformed.campaigns_current WHERE campaignstartdate < {}"
# # timestamp1 = "'2020-06-01 07:30:00.000'"
# # print(query.format(timestamp1))
# #
# # query = "SELECT * FROM leadallocation_conformed.campaigns_current WHERE campaignstartdate < '{}'"
# # timestamp1 = "2020-06-01 07:30:00.000"
# # print(query.format(timestamp1))
# #
# # print('\n')
# # ts = []
#
#
# # start_timestamp = "2020-06-01 07:30:00.000"
# # timestamp1 = "'" + str(start_timestamp) + "'"
#
# # end_timestamp = "2020-06-02 07:30:00.000"
# #
# # start_timestamp = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
# # end_timestamp = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')
# #
# #
# # while start_timestamp < end_timestamp:
# #     ts.append(str(start_timestamp))
# #     start_timestamp = start_timestamp + timedelta(hours=1)
# #
# #     if str(start_timestamp).split()[1]=='01:30:00':
# #         break
# #
# # print(ts)
#
# # query = """  SELECT loannumber, campaignstartdate
# #                             FROM lola_raw_access.campaignchangedevent_historical_vw
# #                             WHERE datekey <= {0} AND campaignstartdate < {0}
# #                             GROUP BY loannumber, campaignstartdate
# #                      """
# #
# #
# # print(query.format(timestamp1))
# #


query = '''
                            SELECT currTbl.attributes_entityid,
                                        currTbl.loannumber,
                                        currTbl.sourcereferenceid,
                                        currTbl.state,
                                        currTbl.prevloanpurpose,
                                        currTbl.iscurrentqlclient,
                                        currTbl.isqlms,
                                        currTbl.monthlycitycountylientaxamount,
                                        currTbl.monthlyhoiamount,
                                        currTbl.dwelling_type,
                                        currTbl.clientincomeannualamount,
                                        currTbl.subjectpropertytype,
                                        currTbl.subjectpropertyoccupancytype,
                                        currTbl.isinvestmentproperty,
                                        currTbl.subjectpropertyliencurrentbalance,
                                        currTbl.subjectpropertylienremainingtermmonths,
                                        currTbl.subjectpropertylienmonthlypayment,
                                        currTbl.subjectpropertylienoriginalbalance,
                                        currTbl.subjectpropertylienoriginaltermmonths,
                                        currTbl.subjectpropertylienoriginalagencytype,
                                        currTbl.subjectpropertypropertyvalue,
                                        currTbl.finalcreditscore,
                                        currTbl.nonmortgagerevolvingdebt,
                                        currTbl.nonmortgagemonthlydebt,
                                        currTbl.state_close_rate,
                                        CASE
                                                WHEN currTbl.purchase_prediction <> newSc.purchase_prediction OR currTbl.refi_prediction <> newSc.refi_prediction
                                                THEN newSc.purchase_prediction
                                                ELSE currTbl.purchase_prediction
                                        END AS purchase_prediction,
                                        CASE
                                                    WHEN currTbl.purchase_prediction <> newSc.purchase_prediction OR currTbl.refi_prediction <> newSc.refi_prediction 
                                                    THEN newSc.refi_prediction
                                                    ELSE currTbl.refi_prediction
                                        END AS refi_prediction,
                                        CASE
                                            WHEN currTbl.purchase_prediction <> newSc.purchase_prediction OR currTbl.refi_prediction <> newSc.refi_prediction 
                                            THEN newSc.purchase_leadgrade
                                            ELSE currTbl.purchase_leadgrade
                                        END AS purchase_leadgrade,
                                        CASE
                                                WHEN currTbl.purchase_prediction <> newSc.purchase_prediction OR currTbl.refi_prediction <> newSc.refi_prediction 
                                                THEN newSc.refi_leadgrade
                                                ELSE currTbl.refi_leadgrade
                                        END AS refi_leadgrade,
                                        CASE
                                                WHEN currTbl.purchase_prediction <> newSc.purchase_prediction OR currTbl.refi_prediction <> newSc.refi_prediction 
                                                THEN newSc.scoring_ts_est
                                                ELSE currTbl.scoring_ts_est
                                        END AS scoring_ts_est                            
                            FROM newScores newSc
                            JOIN {0} currTbl
                                ON currTbl.attributes_entityid = newSc.attributes_entityid

                     '''

print(query.format("model_scoring_outputs_uat.lq_appcall_1_0_1_model_results_current"))