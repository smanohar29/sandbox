DROP TABLE "lead_allocation"."calls_string_dates"; 

CREATE TABLE "lead_allocation"."calls_string_dates"
(
  "loannumber" varchar(15), 
  "callid" int, 
  "commonid" int, 
  "bankercommonid" int, 
  "clientphone" int, 
  "voicemaildatetime" varchar(30), 
  "callstartdatetime"  varchar(30), 
  "callingclientdatetime"  varchar(30), 
  "transferstartdatetime"  varchar(30), 
  "transferflag" int, 
  "transfercompleteflag" int, 
  "mopflag" int, 
  "receiveddate" varchar(10),
  "monthkey"varchar(10) 
  
)
TIER STRATEGY (
( ( VRAM 1, RAM 5, DISK0 5, PERSIST 5 ) )
)