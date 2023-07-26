import re

fieldStrings = 'finalloannumber, loannumber,gcid    ,loanpurpose,leadtypecode,webscore   ,rundate ,modeltype ,armindicator,paymentinfull,unpaidbalance,loanterm,investorshortname,loanphase,loanchannel    ,productgroup,cohort       ,vintage,evermbaflag,ltv_ratio,interestrate,qlproductcode,qlservicingloannumber,bbviable,bbcashoutviable,bblowerpaymentviable,bbshortentermviable,closingdate        ,active_forbearance,loanremainingterm,istexas50a6,seeker2ampinprocessflg,seeeker2lolainprocessflg,submissionengineloansinampflg,closingpast30flg,closingpast60flg,closingpast90flg,closingpast150flg,closingpast180flg,closedeverflg,killed14daysflg,killed30daysflg,killed60daysflg,killed90daysflg,killed150daysflg,killed180daysflg,withdrawn14daysflg,withdrawn30daysflg,withdrawn60daysflg,withdrawn90daysflg,withdrawn150daysflg,withdrawn180daysflg,withdrawnpast2130daysflg,withdrawnpast2160daysflg,withdrawnpast2190daysflg,withdrawnpast21150daysflg,withdrawnpast21180daysflg,deniedpast2130daysflg,deniedpast2160daysflg,deniedpast2190daysflg,deniedpast2150daysflg,deniedpast21180daysflg,deniedpast30daysflg,deniedpast60daysflg,deniedpast90daysflg,deniedpast150daysflg,deniedpast180daysflg,creditpulllast30daysflg,creditpulllast60daysflg,creditpulllast90daysflg,creditpulllast150daysflg,creditpulllast180daysflg,isteammemberflg,vaflg,conventflg,usdaflg,fhaflg,nonemorttypeflg,optouttcpaflg,qlmsflg,optoutflg,attributes_entityid                 ,phone_number,serviceflg,allyflg,bbiseligible,original_loan_amount,pty_state,mosaichousehold,nonmortgagemonthlydebt,nonmortgagerevolvingdebt,ltv_ratio_current_estimated,processeddate,equity  ,BlueRainHomeImpFlag,MajCCUserFlag,GTM_CO_Segment    ,GTM_CO_RKT,Play_specific,play,InvestorFlag,LastPlay,LastSubmitDate,payoff_percentile,payoff_bucket,PayoffProb         ,retention_percentile,retention_bucket,RetentionProb      ,OptOutFlag,conversion'
fieldStrings = re.sub('\s+', '', fieldStrings).strip()
print(fieldStrings)

print('\n')
fieldList = list(fieldStrings.split(","))
print(fieldList)

print('\n')
for field in fieldList:
    print('{"Name" : "' + field + '", "Type" : "String"},')


partitionColumns = ["datekey", "hour"]
print(partitionColumns)

cols = ','.join([str(elem) for elem in partitionColumns])
print(cols)

partitionColumns = ["datekey"]
print(partitionColumns)

cols = ','.join([str(elem) for elem in partitionColumns])
print(cols)

#####################################################################################################################

from datetime import datetime, timedelta, timezone
import pytz

now = datetime.utcnow()
tz = pytz.timezone('US/Eastern')
tzoffset = tz.utcoffset(now)
currDateTime = str(now + tzoffset)

print(currDateTime)
print(currDateTime.split(' ')[0])

#####################################################################################################################

auditTable = 'rktdp_audit_access.digital_data_alert_count_audit'
datekey = '2022-12-23'

alter_stmt = "alter table {0} add if not exists partition(datekey='{1}')".format(auditTable, datekey)
print(alter_stmt)

totalCountLookupVal = '2023-01-27'
partition1 = '2023-01-30'

partition1 = datetime.strptime(partition1, '%Y-%m-%d')
# print(partition1)

partition1MinusOneDay = str(partition1 - timedelta(days=1)).split(' ')[0]
partition1MinusTwoDays = str(partition1 - timedelta(days=2)).split(' ')[0]

print(partition1MinusOneDay)
print(partition1MinusTwoDays)

if totalCountLookupVal not in (partition1, partition1MinusOneDay, partition1MinusTwoDays):
    print("partition mismatch")
else:
    print("no error")

targetTables = [
            "rktdp_adobe_omniture_raw_access.adobe_rawdata_hourly",
            "rktdp_adobe_omniture_raw_access.browser_hourly",
            "rktdp_adobe_omniture_raw_access.browser_type_hourly"]

for targetTable in targetTables:
    x = targetTable.split('.')[1]
    print(x)