Create view lead_allocation.calls_vw   as 

select 
    loannumber, 
    callid, 
    commonid, 
    bankercommonid, 
    clientphone, 
    cast (  voicemaildatetime as datetime) as voicemaildatetime, 
    cast (  callstartdatetime as datetime) as callstartdatetime, 
    cast (  callingclientdatetime as datetime) as callingclientdatetime, 
    cast (  transferstartdatetime as datetime) as transferstartdatetime, 
    transferflag, 
    transfercompleteflag, 
    mopflag, 
    receiveddate, 
    monthkey
from "lead_allocation"."calls_string_dates" 

