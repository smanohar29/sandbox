DROP TABLE "lead_allocation"."calls"; 

CREATE TABLE "lead_allocation"."calls"
(
  "loannumber" varchar(15), 
  "callid" int, 
  "commonid" int, 
  "bankercommonid" int, 
  "clientphone" int, 
  "voicemaildatetime" timestamp, 
  "callstartdatetime" timestamp, 
  "callingclientdatetime" timestamp, 
  "transferstartdatetime" timestamp, 
  "transferflag" tinyint, 
  "transfercompleteflag" tinyint, 
  "mopflag" tinyint, 
  "receiveddate" varchar(10),
  "monthkey" varchar(10)
  
)
TIER STRATEGY (
( ( VRAM 1, RAM 5, DISK0 5, PERSIST 5 ) )
)