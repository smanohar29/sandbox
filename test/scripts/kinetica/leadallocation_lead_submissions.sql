CREATE TABLE "lead_allocation"."leadsubmissions"
(
"loannumber"  varchar(10),
"h_tlbfirsttimehomebuyer"  varchar(5),
"h_twoupb"  varchar(20),
"h_tadproperty"  varchar(87),
"h_tzip"  varchar(5),
"h_sstate"  varchar(2),
"h_tcity"  varchar(28),
"h_ttotmthlydebt"  varchar(9),
"h_tppresentval"  varchar(21),
"h_tcashout"  varchar(17),
"h_tbaseloanamt"  varchar(18),
"h_tyearhomebought"  varchar(10),
"h_tisservicing"  varchar(5),
"h_scurrrate"  varchar(9),
"h_tcurrbal"  varchar(19),
"h_tsalesprice"  varchar(16),
"h_trealestateagent"  varchar(5),
"h_lisva"  varchar(7),
"h_sselfcreditrating"  varchar(5),
"h_tprptytypcd"  varchar(35),
"h_tcontactinfuture"  varchar(5),
"addressstatus"  varchar(11),
"typeoflead"  varchar(11),
"h_temail"  varchar(74),
"h_sloan"  varchar(29),
"h_teveareaphone"  varchar(3),
"h_tfirsttimehomebuyer"  varchar(4),
"h_tgoals"  varchar(26),
"h_thearaboutuscd"  varchar(44),
"h_tupemclassic"  varchar(22),
"h_tleasingmonth"  varchar(2),
"h_tleasingyear"  varchar(4),
"h_tleasing"  varchar(5),
"h_toffer"  varchar(14),
"h_tdownpayment"  varchar(16),
"h_thome_search_status"  varchar(40),
"h_twebcredit"  varchar(13),
"h_torigmorttype"  varchar(13),
"h_tleadtype"  varchar(15),
"h_tconfirmationemail"  varchar(16),
"h_tcurrentdate"  varchar(22),
"h_tupemgrade"  varchar(0),
"upemmodeltype"  varchar(16),
"h_tvenleadid"  varchar(96),
"correlationid"  varchar(36),
"h_tfname"  varchar(12),
"h_tlname"  varchar(13),
"h_tb1nickname"  varchar(12),
"h_tb2fname"  varchar(12),
"h_tb2lname"  varchar(12),
"h_tb2nickname"  varchar(12),
"h_tb2email"  varchar(74),
"h_tstreet"  varchar(42),
"h_tb2street"  varchar(62),
"h_tb2city"  varchar(29),
"h_tb2state"  varchar(2),
"h_tb2zip"  varchar(5),
"h_tmthlyincome"  varchar(288),
"h_tb2mthlyincome"  varchar(19),
"canshowproofofincome"  varchar(5),
"h_tisdeduped"  varchar(5),
"h_temployeryears"  varchar(4),
"h_temployername"  varchar(86),
"h_tselfemployed"  varchar(7),
"h_tb2employeryears"  varchar(4),
"h_tb2employername"  varchar(67),
"h_tselfemployed2"  varchar(3),
"employmentstatus"  varchar(12),
"h_tispropertyowner"  varchar(5),
"h_tb2ispropertyowner"  varchar(5),
"h_tisleasing"  varchar(20),
"h_trentpayment"  varchar(12),
"h_townrent"  varchar(9),
"h_tdesiredpayment"  varchar(18),
"h_tmopi"  varchar(8),
"h_tauthcode"  varchar(248),
"contactbyrealestateagent"  varchar(5),
"agentid"  varchar(9),
"corporatedirect"  varchar(5),
"hasclientrefinancedbefore"  varchar(17),
"h_tinitintrrate"  varchar(6),
"h_tprevcashoutamt"  varchar(10),
"h_tprevrefipurpose"  varchar(24),
"currentloanvendor"  varchar(23),
"ratetype"  varchar(10),
"cohortproduct"  varchar(14),
"desiredratetype"  varchar(9),
"h_tpurchaseagreement"  varchar(5),
"h_tlbsignedpurchaseagreement"  varchar(5),
"h_tleadcost"  varchar(17),
"leadduration"  varchar(8),
"h_ssource"  varchar(44),
"leadsourcecategorycode"  varchar(42),
"sourceid"  varchar(100),
"subsource"  varchar(192),
"ishighpriority"  varchar(5),
"issweepstakes"  varchar(4),
"upemversion"  varchar(6),
"h_tupemcurrent"  varchar(17),
"h_tpromotioncode"  varchar(17),
"h_thasforeclosure"  varchar(5),
"h_thasbankruptcy"  varchar(5),
"h_tb2hasforeclosure"  varchar(5),
"h_tb2hasbankrutpcy"  varchar(5),
"h_cnosol"  varchar(5),
"h_tprimaryresidence"  varchar(20),
"h_tb2primaryresidence"  varchar(20),
"propertyuse"  varchar(19),
"h_ttimeframetobuy"  varchar(20),
"h_tlbtimeframetobuy"  varchar(61),
"foundnewhome"  varchar(5),
"referredbycommonid"  varchar(5),
"h_tcommonid"  varchar(5),
"h_trefersid"  varchar(8),
"testtag"  varchar(9),
"h_tnetbenltv"  varchar(10),
"h_tnetbenbaseamount"  varchar(12),
"h_tisavsvalidated"  varchar(5),
"h_tbday3"  varchar(4),
"h_tb2day3"  varchar(4),
"h_tmarital"  varchar(9),
"h_tbmarital"  varchar(20),
"h_tcurrpmt"  varchar(38),
"h_tdownpaymentborrowed"  varchar(20),
"numberoflatepayments"  varchar(2),
"isdisasterarea"  varchar(4),
"disasterdescription"  varchar(23),
"callcenter"  varchar(4),
"totalhops"  varchar(1),
"purchaseintentcode"  varchar(1),
"leadcreatedate"  DATETIME,
"h_tdupsystemname"  varchar(27),
"h_tcellphone"  varchar(10),
"h_tadditionalphone1"  varchar(10),
"h_tadditionalphone2"  varchar(10),
"h_tdayareaphone"  varchar(3),
"h_tdayprefixphone"  varchar(3),
"h_tdaylast4phone"  varchar(4),
"h_teveprefixphone"  varchar(3),
"h_tevelast4phone"  varchar(4),
"h_tb2cellphone"  varchar(10),
"h_tb2dayareaphone"  varchar(3),
"h_tb2dayprefixphone"  varchar(3),
"h_tb2daylast4phone"  varchar(4),
"h_tb2eveareaphone"  varchar(3),
"h_tb2eveprefixphone"  varchar(3),
"receiveddate"  varchar(10),
"monthkey"  varchar(10)
)
TIER STRATEGY (
( ( VRAM 1, RAM 5, DISK0 5, PERSIST 5 ) )
)