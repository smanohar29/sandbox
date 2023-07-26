from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.functions import udf, when, col, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, max, sum, count as tcount
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as func


################################################# features_helper #################################################

def get_leadcreatehour(timestamp):
    if timestamp is None:
        return -1
    else:
        timestamp = modifytimeformat(timestamp)
        result = try_parsing_date(timestamp)
        return result.hour

# Leadtimebucket
def get_leadtimebucket(leadcreatehour):
    leadcreatehour = int(leadcreatehour)
    if leadcreatehour == -1: return 'missing'
    if 0 <= leadcreatehour <= 3: return 'weemorning'
    if 4 <= leadcreatehour <= 7: return 'earlymorning'
    if 8 <= leadcreatehour <= 11: return 'morning'
    if leadcreatehour == 12: return 'lunchtime'
    if 13 <= leadcreatehour <= 15: return 'afternoon'
    if 16 <= leadcreatehour <= 19: return 'evening'
    if leadcreatehour >= 20:
        return 'night'
    else:
        return 'unknown'

# Loanpurpose
def get_loanpurpose(h_sloan):
    if h_sloan is not None:
        h_sloan = h_sloan.lower()
        if h_sloan not in ('mortgagefirst', 'purchase', 'offerpending-foundahouse', 'construction-permane',
                           'signedapurchaseagreement'): return 'Refinance'
        if h_sloan in ('mortgagefirst', 'purchase', 'offerpending-foundahouse', 'construction-permane',
                       'signedapurchaseagreement'): return 'Purchase'
    else:
        return h_sloan

# Leadtypebucket
def get_leadtypebucket(h_tleadtype, h_tvenleadid, h_tadproperty):
    if h_tleadtype is None: h_tleadtype = ''
    if h_tvenleadid is None: h_tvenleadid = ''
    if h_tadproperty is None: h_tadproperty = ''

    h_tleadtype = h_tleadtype.lower()
    h_tvenleadid = h_tvenleadid.lower()
    h_tadproperty = h_tadproperty.lower()

    if h_tleadtype == 'rlhap': return 'rocket'
    if h_tleadtype.startswith('rl'): return 'rocketloans'
    if h_tleadtype == 'qlms': return 'qlms'
    if h_tleadtype == 'lbqldrlp': return 'lmb_lp'
    if h_tleadtype == 'lmblpv3': return 'lmbstyle_lp'
    if 'chat' in h_tleadtype: return 'chat'
    if h_tleadtype.startswith('lt'): return 'lendingtree1'
    if h_tleadtype.startswith('gslt'): return 'lendingtree2'
    if h_tleadtype == 'conql': return 'leadbuy_conql'
    if h_tleadtype.startswith('cari') or h_tleadtype == 'aqally': return 'cari'
    if h_tleadtype.startswith('pga') or h_tleadtype.startswith('hio') or h_tleadtype.startswith(
            'mlb') or h_tleadtype.startswith('dhaw') or h_tleadtype.startswith('ladya') or h_tleadtype.startswith(
        'hgtv') or h_tleadtype.startswith('zb') or h_tleadtype.startswith('hlbpzing') or h_tleadtype.startswith(
        'tctts') or h_tleadtype.startswith('zcss'): return 'sweeps'
    if h_tvenleadid == 'false': return 'leadbuy'
    if len(h_tadproperty) > 1: return 'web'
    if len(h_tvenleadid) > 1:
        return 'leadbuy'
    else:
        return 'other'

# leadagedays
# TODO change to accept a start and end date and make sure it accepts strings and datetimes to make this more testable \ usable

def get_leadagedays(createdate):
    if createdate is not None:
        #             createdate = parser.parse(createdate)
        createdate = str(createdate).rsplit(".")[0]
        createdate = try_parsing_date(createdate)
        #         createdate = datetime.strptime(createdate, '%m/%d/%Y %H:%M:%S')
        currentdate = datetime.now()

        leadage = currentdate - createdate
        leadagedays = leadage.days + (leadage.seconds / (60 * 60 * 24))

        return leadagedays
    else:
        return 0


# TODO move to date_helper and make more generic
def get_leadagedays_training(createdate, calldate):
    if createdate is not None and calldate is not None:
        createdate = try_parsing_date(createdate)
        calldate = try_parsing_date(str(calldate))

        leadage = calldate - createdate
        leadagedays = leadage.days + (leadage.seconds / (60 * 60 * 24))

        return leadagedays
    else:
        return 0


# hourofdial
# TODO move to datehelper and make more generic
def get_hourofdial(callstarttime):
    if callstarttime is None:
        return -1

    callstarttime = str(callstarttime).rsplit(".")[0]
    startdate = datetime.strptime(callstarttime, '%Y-%m-%d %H:%M:%S')

    if startdate.hour < 8:
        return 8
    else:
        return startdate.hour

# dialtimebucket
# TODO move to datehelper and make more generic
def get_dialtimebucket(hourofdial):
    if hourofdial == -1: return 'missing'
    if 0 <= hourofdial <= 3: return 'weemorning'
    if 4 <= hourofdial <= 7: return 'earlymorning'
    if 8 <= hourofdial <= 11: return 'morning'
    if hourofdial == 12: return 'lunchtime'
    if 13 <= hourofdial <= 15: return 'afternoon'
    if 16 <= hourofdial <= 19: return 'evening'
    if hourofdial >= 20:
        return 'night'
    else:
        return 'unknown'

# calldateid
# TODO replace with getdateid from date_helper
def get_calldateid(callstarttime):
    calldateid = str(callstarttime)
    calldateid = calldateid.rsplit(" ")[0]
    calldateid = calldateid.replace('-', '')
    return str(calldateid)

# previousdaycalldateid
# TODO create general function to get previous dateid
def get_previousdaycalldateid(callstarttime):
    if callstarttime is not None:
        previousdaycalldateid = str(callstarttime)
        previousdaycalldateid = previousdaycalldateid.rsplit(" ")[0]
        previousdaycalldateid = datetime.strptime(previousdaycalldateid, '%Y-%m-%d')
        previousdaycalldateid = previousdaycalldateid - timedelta(hours=24)
        previousdaycalldateid = str(previousdaycalldateid).replace('-', '')
        previousdaycalldateid = previousdaycalldateid.rsplit(" ")[0]
        return previousdaycalldateid
    else:
        return callstarttime

def convertweekendflg(weekendflg):
    if weekendflg == True:
        return 1
    else:
        return 0


def convertholidayflg(holidayflg):
    if holidayflg == True:
        return 1
    else:
        return 0


def add_one_to_prevcallcall(previouscallscount):
    previouscallscount = int(previouscallscount)
    previouscallscount = previouscallscount + 1
    return str(previouscallscount)


#####################################################################################################################################################

def get_sweepstakesflag(h_tleadtype):
    h_tleadtype = h_tleadtype.lower()
    if h_tleadtype.startswith('pga'): return 1
    if h_tleadtype.startswith('hio'): return 1
    if h_tleadtype.startswith('mlb'): return 1
    if h_tleadtype.startswith('dhaw'): return 1
    if h_tleadtype.startswith('ladya'): return 1
    if h_tleadtype.startswith('hgtv'): return 1
    if h_tleadtype.startswith('zb'): return 1
    if h_tleadtype.startswith('hlbpzing'): return 1
    if h_tleadtype.startswith('tctts'): return 1
    if h_tleadtype.startswith('zcss'): return 1
    else: return 0

# dateid
# TODO replace with getdateid from date_helper
def get_dateid(callstarttime):
    callstarttime = str(callstarttime).rsplit(" ")[0]
    result = callstarttime.replace("-", "")
    return result


# TODO - this could just check for existance of closing date as long as the closing date is filtered in the source query
def get_closingflag(closingdate):
    closingdate = datetime.strptime(closingdate, '%Y-%m-%dT%H:%M:%S.%f')
    if closingdate < datetime.now():
        closingflag = 1
    else:
        closingflag = 0
    return closingflag


def marketingpartner(dataframe):
    """This function duplicates the marketingpartner code that is done in SQL to assign lead type buckets based on the
    given lead type. Logic can be found in Hue.
    Args:
        dataframe: PySpark dataframe
    Returns:
        Updated dataframe with new 'marketing_partner' column.
    """
    # dataframe = convertDataframeColsToLower(dataframe, [h_tleadtype])
    dataframe = dataframe.withColumn('marketing_partner', F.when(F.col('h_tleadtype').like('brql%'), 'bestratereferral')
                                     .when(F.col('h_tleadtype').like('qlbl%'), 'bills_qlbl_mediaforce')
                                     .when(F.col('h_tleadtype').like('hacql%'), 'bills_hacql')
                                     .when(F.col('h_tleadtype').like('bcql%'), 'brokermatch')
                                     .when(F.col('h_tleadtype').like('frql%'), 'freerateupdate')
                                     .when(F.col('h_tleadtype').like('fbql%'), 'fullbeaker')
                                     .when(F.col('h_tleadtype').like('gslt%'), 'getsmart_gslt')
                                     .when(F.col('h_tleadtype').like('gsql%'), 'getsmart_gsql_other')
                                     .when(F.col('h_tleadtype').like('qlftyr%'), 'guidetolenders_conql_qlftyr')
                                     .when(F.col('h_tleadtype').like('qlmty%'), 'guidetolenders_qlmty')
                                     .when(F.col('h_tleadtype').like('qsql%'), 'guidetolenders_qsql')
                                     .when(F.col('h_tleadtype').like('conql%'), 'guidetolenders_conql_qlftyr')
                                     .when(F.col('h_tleadtype').like('hoql%'), 'homes')
                                     .when(F.col('h_tleadtype').like('lcql%'), 'leadcloud')
                                     .when(F.col('h_tleadtype').like('ptql%'), 'leadpoint')
                                     .when(F.col('h_tleadtype').like('ltql%'), 'lendingtree')
                                     .when(F.col('h_tleadtype').like('lbql%'), 'lowermybills')
                                     .when(F.col('h_tleadtype').like('mfql%'), 'bills_qlbl_mediaforce')
                                     .when(F.col('h_tleadtype').like('nmql%'), 'netmoneywizard')
                                     .when(F.col('h_tleadtype').like('quizzle%'), 'quizzle')
                                     .when(F.col('h_tleadtype').like('acql%'), 'ratemarketplace_saxumpartners')
                                     .when(F.col('h_tleadtype').like('rgfql%'), 'reallygreatrate')
                                     .when(F.col('h_tleadtype').like('rgql%'), 'reallygreatrate')
                                     .when(F.col('h_tleadtype').like('rmql%'), 'revimedia')
                                     .when(F.col('h_tleadtype').like('saxqlpl%'), 'ratemarketplace_saxumpartners')
                                     .when(F.col('h_tleadtype').like('lwql%'), 'smartquote')
                                     .when(F.col('h_tleadtype').like('tlnqlp%'), 'thelendersnetwork')
                                     .otherwise('other'))

    return dataframe


from datetime import datetime
from dateutil import tz, parser
import pytz


################################################# #### date_helper #################################################

def utcnow():
    return datetime.utcnow()


def estnow():
    tz = pytz.timezone('US/Eastern')
    now = datetime.utcnow()

    tzoffset = tz.utcoffset(now)
    return now + tzoffset


def setDateTimetoEST(datetime):
    est = pytz.timezone('US/Eastern')

    return datetime.replace(tzinfo=est)


def try_parsing_date(text):
    if type(text) is datetime:
        return text

    text = text[:21]
    # return parser.parse(text)
    for fmt in (
    '%m/%d/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f", '%m/%d/%Y %H:%M:%S.%f'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found - ' + text)


def getDatebyTimezone(timezone, dateFormat):
    # ex formats - "%Y-%m-%d" (partitions) , "%m/%d/%Y" - (ETLRunmanager-nextRunDate), "%Y%m%d" - (ETLRunmanager-nextRunDateID)

    if timezone == 'EST':
        return datetime.strftime(estnow(), dateFormat)

    if timezone == 'UTC':
        return datetime.strftime(utcnow(), dateFormat)


def getTimeUnitFromDateTimeDelta(timedelta_delay, unitofmeasurement):
    days, seconds = timedelta_delay.days, timedelta_delay.seconds
    hours = days * 24 + seconds // 3600
    minutes = days * 1440 + seconds // 60
    # minutes = days * 1440 + hours * 60 + seconds % 60
    # seconds = seconds % 60

    if unitofmeasurement == 'days':
        return days

    if unitofmeasurement == 'hours':
        return hours

    if unitofmeasurement == 'minutes':
        return minutes

    # TODO make this accept either datetime or string - currently is supports string only.


def converttoDateID(date):
    if date is not None:
        dateID = parser.parse(date).strftime('%Y%m%d')
        return int(dateID)


# hour
def get_hour(datetime):
    if datetime is None:
        return -1
    else:
        #         callstarttime = str(datetime).rsplit(".")[0]
        #         startdate = datetime.strptime(datetime, '%Y-%m-%d %H:%M:%S')
        return datetime.hour


def modifytimeformat(timestamp):
    if timestamp is None:
        return timestamp
    else:
        timestamp = timestamp.rsplit(".")[0]
        #     timestamp = timestamp.replace("T"," ")
        #     timestamp = timestamp.replace("-","/")
        return timestamp


# dateid
def get_dateid(callstartdatetime):
    callstartdatetime = str(callstartdatetime).rsplit(" ")[0]
    result = callstartdatetime.replace("-", "")
    return result


def get_targetreceiveddate(receiveddate):
    targetreceiveddate = str(receiveddate).rsplit(" ")[0]
    return targetreceiveddate

################################################# geography_helper #################################################

# Timezone
def get_timezone(h_sstate):
    h_sstate = str(h_sstate).upper()
    if h_sstate in ('WA', 'OR', 'CA', 'NV'):
        return 'PST'
    elif h_sstate in ('MT', 'WY', 'UT', 'CO', 'AZ', 'NM', 'ND', 'SD', 'NE', 'KS', 'TX', 'ID'):
        return 'MST'
    elif h_sstate in ('MN', 'IA', 'MO', 'AR', 'OK', 'MS', 'AL', 'TN', 'IL', 'WI', 'LA'):
        return 'CST'
    elif h_sstate in (
            'ME', 'NH', 'VT', 'MA', 'NY', 'RI', 'CT', 'NJ', 'DE', 'MD', 'PA', 'WV', 'OH', 'MI', 'IN', 'KY', 'VA', 'NC',
            'SC',
            'GA', 'FL', 'DC', ):
        return 'EST'
    elif h_sstate in ('HI'):
        return 'Hawaii'
    elif h_sstate in ('AK'):
        return 'Alaska'
    else:
        return 'Other'

# localhour
def get_localhour(timezone, leadcreatehour):
    leadcreatehour = int(leadcreatehour)
    if leadcreatehour == -1: return -1
    if 'EST' in timezone:  leadcreatehour = leadcreatehour - 0
    if 'CST' in timezone:  leadcreatehour = leadcreatehour - 1
    if 'MST' in timezone:  leadcreatehour = leadcreatehour - 2
    if 'PST' in timezone:  leadcreatehour = leadcreatehour - 3
    if 'Alaska' in timezone:  leadcreatehour = leadcreatehour - 4
    if 'Hawaii' in timezone:  leadcreatehour = leadcreatehour - 5
    if 'Other' in timezone: leadcreatehour = leadcreatehour - 0

    if leadcreatehour < 0:
        return leadcreatehour + 24
    else:
        return leadcreatehour

################################################# email_helper #################################################

# Clientemaildomain
def get_clientemaildomain(h_temail):
    if h_temail is None:
        result = 'missing'
    else:
        test = h_temail
        start = test.find('@')
        result = test[start:len(test)]
    return result


# Clientemailtopleveldomain
def get_clientemailtopleveldomain(h_temail):
    if h_temail is None:
        result = 'missing'
    else:
        test = h_temail
        start = test.find('@')
        end = test.find(".", start)
        result = test[end:len(test)]
    return result