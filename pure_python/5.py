# import random
#
# print(round(random.uniform(0.1, 1.0), 10))
#
#
#
# test = '''{"TimeStamp":"09/10/2019 00:00:01.852-04:00","Key":"LeadAugmentedEvent","MessageSource":"SubmissionEngine","Payload":{"LoanNumber":12346,"CorrelationId": "9bd68aee-4f17-4d7d-a6d6-9d0dcc9e0eed","H_tLeadType":"RLMAN","H_tadProperty":"","H_tVenLeadId":123, "H_sLoan":"purchase", "H_tStreet":"123 wings st", "H_tFname":"Steve","H_tLname":"Yzerman", "H_tEmail":"SteveYzerman@gmail.com", "H_sState":"MI","H_tZip":"48201","H_tEveAreaPhone":"313","H_tDupSystemName":null,"H_tIsServicing":false,"H_lisVA":"no","H_tPrptyTypCd":null,"H_tContactInFuture":"False","AddressStatus":"Approved","TypeOfLead":"Website","H_tOrigMortType":null,"H_tpPresentVal":null,"H_tCashOut":null,"H_tBaseLoanAmt":65000.0,"H_sCurrRate":null,"H_tCurrBal":65000.0,"H_sSelfcreditrating":null,"H_tLeadCost":null,"LeadDuration":null,"TotalHops":null,"H_tRealEstateAgent":null,"H_tLBTimeFrametoBuy":null,"FoundNewHome":null,"H_tFirstTimeHomeBuyer":"Yes","H_tPurchaseAgreement":null,"H_tLBSignedPurchaseAgreement":null,"H_tSalesPrice":70000.0,"h_tHasForeclosure":false,"h_tHasBankruptcy":false,"H_cNoSol":null,"H_tOffer":null,"H_thome_search_status":"Researching Options","H_tWebCredit":"Poor","H_tHearAboutUsCd":null,"H_tConfirmationEmail":null,"CanShowProofOfIncome":null, "h_tlbfirsttimehomebuyer":null, "h_tdownpayment":null}}'''
# test = test.lower()
#
#
# new_test = '''{"loannumber":12346,"correlationid": "9bd68aee-4f17-4d7d-a6d6-9d0dcc9e0eed","h_tleadtype":"rlman","h_tadproperty":"","h_tvenleadid":123, "h_sloan":"purchase", "h_tstreet":"123 wings st", "h_tfname":"steve","h_tlname":"yzerman", "h_temail":"steveyzerman@gmail.com", "h_sstate":"mi","h_tzip":"48201","h_teveareaphone":"313","h_tdupsystemname":null,"h_tisservicing":false,"h_lisva":"no","h_tprptytypcd":null,"h_tcontactinfuture":"false","addressstatus":"approved","typeoflead":"website","h_torigmorttype":null,"h_tppresentval":null,"h_tcashout":null,"h_tbaseloanamt":65000.0,"h_scurrrate":null,"h_tcurrbal":65000.0,"h_sselfcreditrating":null,"h_tleadcost":null,"leadduration":null,"totalhops":null,"h_trealestateagent":null,"h_tlbtimeframetobuy":null,"foundnewhome":null,"h_tfirsttimehomebuyer":"yes","h_tpurchaseagreement":null,"h_tlbsignedpurchaseagreement":null,"h_tsalesprice":70000.0,"h_thasforeclosure":false,"h_thasbankruptcy":false,"h_cnosol":null,"h_toffer":null,"h_thome_search_status":"researching options","h_twebcredit":"poor","h_thearaboutuscd":null,"h_tconfirmationemail":null,"canshowproofofincome":null, "h_tlbfirsttimehomebuyer":null, "h_tdownpayment":null}'''
# print(new_test)
#
# start = new_test.find('"')
# end = new_test.find(':')
#
# for i in range(0, len(new_test)):
#    new_test.find('"', start, end)
#    print(new_test[start: end])
#
#    start = end


# i=1
# path=f'hdfs://datalakeprod/uat/DataScienceSandbox/Maestro/ContactMethod/logs/hdfs/logs_iteration{i}.csv'
# print(path)

# epoch=1
# save_path = 'hdfs://datalakeprod/uat/DataScienceSandbox/Maestro/ContactMethod/model/model_{}.h5'
# model_path = save_path.format(epoch)
# print(" MODEL SAVED {} - ".format(model_path))
#
#
# epoch=1
# save_path = f'hdfs://datalakeprod/uat/DataScienceSandbox/Maestro/ContactMethod/model/iteration_{epoch}/model_{epoch}.h5 - '
# print(" MODEL SAVED " + save_path)

import re

epoch=1
run_no=2

# save_path = "hdfs://datalakeprod/uat/DataScienceSandbox/Maestro/ContactMethod/model/iteration_{0}/model_{1}.h5"
# model_path = save_path.replace(('{0}'),str(run_no)).replace('{1}',str(epoch))
# # print(save_path)
# # model_path = save_path.format(epoch)
# print(" MODEL SAVED -> " + model_path)

# save_path = "hdfs://datalakeprod/uat/DataScienceSandbox/Maestro/ContactMethod/model/iteration_{0}/model_{1}.h5"
# model_path = save_path.format(run_no,epoch)
#
# print(" MODEL SAVED -> " + model_path)


config1 = {
    "stagingBucketName":"ql-dl-offline-featurestore-uat-814128445328-us-east-2",
    "stagingFileBasePrefix":"offline_tables/RocketScience/LeadAllocation/LeadQuality/lq_conversion_current_uat",
    "gatewayBucketName":"prod-206823-leadquality-ql-output",
    "gatewayFilePrefix":"test",
    "timezone": "US/Eastern",
    "dateformat": "",
    "filesConfig":
        {
        "scores": {
            "sourceFolder": "",
            "destinationFileName": "maestro_leadquality_scores",
            "destinationFileExt": ".csv"
        }
    }
}

config2 = {
    "stagingBucketName":"ql-dl-offline-featurestore-uat-814128445328-us-east-2",
    "stagingFileBasePrefix":"offline_tables/RocketScience/LeadAllocation/LeadQuality/lq_conversion_current_uat",
    "gatewayBucketName":"prod-206823-leadquality-ql-output",
    "gatewayFilePrefix":"test",
    "timezone": "US/Eastern",
    "dateformat": "",
    "filesConfig":
        {
        "scores": {
            "sourceFolder": "",
            "destinationFileName": "maestro_leadquality_scores",
            "destinationFileExt": ".csv"
        }
    }
}

stagingBucketName =  config['stagingBucketName']
stagingFileBasePrefix = config['stagingFileBasePrefix']

s3path = "s3a://{0}/{1}/".format(stagingBucketName, stagingFileBasePrefix)
print(s3path)

s3path = "s3a://prod-206823-leadquality-ql-output/"
print(s3path)



















