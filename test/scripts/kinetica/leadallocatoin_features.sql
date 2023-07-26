--historical_conversion 
    --LTC_close_rate
    --state_close_rate
    --adproperty_close_rate
--campaign
    --escalationcount
    --preesc_previouscallcount
    --preesc_previoustransfercount
--lead 
    --leadage 
    --pull from vw    
        --get_localhour ( pull from lead_submissions_vw) 
        --way to handle local hour without even more views ( test with udf ? )
            -- def get_localhour(timezone, leadcreatehour):
            -- leadcreatehour = int(leadcreatehour)
            -- if leadcreatehour == -1: return -1
            -- if 'EST' in timezone:  leadcreatehour = leadcreatehour - 0
            -- if 'CST' in timezone:  leadcreatehour = leadcreatehour - 1
            -- if 'MST' in timezone:  leadcreatehour = leadcreatehour - 2
            -- if 'PST' in timezone:  leadcreatehour = leadcreatehour - 3
            -- if 'Alaska' in timezone:  leadcreatehour = leadcreatehour - 4
            -- if 'Hawaii' in timezone:  leadcreatehour = leadcreatehour - 5
            -- if 'Other' in timezone: leadcreatehour = leadcreatehour - 0

            -- if leadcreatehour < 0:
            --     return leadcreatehour + 24
            -- else:
            --     return leadcreatehour
--call 
    --get_hourofdial
    --get_dialtimebucket
    
SELECT 
     ls.loannumber, 
     ls.leadcreatedate, 
     CASE 
        WHEN ls.h_sloan NOT IN ('mortgagefirst', 'purchase', 'offerpending-foundahouse', 'construction-permane','signedapurchaseagreement') THEN 'Refinance'
        WHEN ls.h_sloan IN ('mortgagefirst', 'purchase', 'offerpending-foundahouse', 'construction-permane','signedapurchaseagreement') THEN 'Purchase'
        ELSE ls.h_sloan END AS LoanPurpose,
    CASE 
        WHEN ls.h_tleadtype = 'rlhap' THEN 'rocket'
        WHEN STARTS_WITH('rl', ls.h_tleadtype) = 1 THEN 'rocketloans'
        WHEN ls.h_tleadtype = 'qlms' THEN 'qlms'
        WHEN ls.h_tleadtype = 'lbqldrlp' THEN 'lmb_lp'
        WHEN ls.h_tleadtype = 'lmblpv3' THEN 'lmbstyle_lp'
        WHEN CONTAINS ('chat', ls.h_tleadtype) = 1  THEN 'chat'
        WHEN STARTS_WITH('lt', ls.h_tleadtype) = 1 THEN 'lendingtree1'
        WHEN STARTS_WITH('gslt', ls.h_tleadtype) = 1 THEN 'lendingtree2'
        WHEN ls.h_tleadtype = 'conql' THEN 'leadbuy_conql'
        WHEN STARTS_WITH('cari', ls.h_tleadtype) = 1 OR ls.h_tleadtype = 'aqally' THEN 'cari'
        WHEN  STARTS_WITH('pga', ls.h_tleadtype) = 1 
            OR STARTS_WITH('hio', ls.h_tleadtype) = 1
            OR STARTS_WITH('mlb', ls.h_tleadtype) = 1
            OR STARTS_WITH('dhaw', ls.h_tleadtype) = 1
            OR STARTS_WITH('ladya', ls.h_tleadtype) = 1 
            OR STARTS_WITH('hgtv', ls.h_tleadtype) = 1
            OR STARTS_WITH('zb', ls.h_tleadtype) = 1 
            OR STARTS_WITH('hlbpzing', ls.h_tleadtype) = 1
            OR STARTS_WITH('tctts', ls.h_tleadtype) = 1
            OR STARTS_WITH('zcss', ls.h_tleadtype) = 1 
            THEN 'sweeps'
        WHEN ls.h_tvenleadid = 'false' THEN 'leadbuy'
        WHEN LENGTH(ls.h_tadproperty) > 1 THEN 'web'
        WHEN LENGTH(ls.h_tvenleadid) > 1 THEN 'leadbuy'
        ELSE 'other'
        END AS leadtypebucket, 
    CASE 
        WHEN STARTS_WITH('brql', ls.h_tleadtype) = 1 THEN 'bestratereferral' 
        WHEN STARTS_WITH('qlbl', ls.h_tleadtype) = 1 THEN 'bills_qlbl_mediaforce' 
        WHEN STARTS_WITH('hacql', ls.h_tleadtype) = 1 THEN 'bills_hacql' 
        WHEN STARTS_WITH('bcql', ls.h_tleadtype) = 1 THEN 'brokermatch' 
        WHEN STARTS_WITH('frql', ls.h_tleadtype) = 1 THEN 'freerateupdate' 
        WHEN STARTS_WITH('fbql', ls.h_tleadtype) = 1 THEN 'fullbeaker' 
        WHEN STARTS_WITH('gslt', ls.h_tleadtype) = 1 THEN 'getsmart_gslt' 
        WHEN STARTS_WITH('gsql', ls.h_tleadtype) = 1 THEN 'getsmart_gsql_other' 
        WHEN STARTS_WITH('qlftyr', ls.h_tleadtype) = 1 THEN 'guidetolenders_conql_qlftyr'
        WHEN STARTS_WITH('qlmty', ls.h_tleadtype) = 1 THEN 'guidetolenders_qlmty' 
        WHEN STARTS_WITH('qsql', ls.h_tleadtype) = 1 THEN 'guidetolenders_qsql'
        WHEN STARTS_WITH('conql', ls.h_tleadtype) = 1 THEN 'guidetolenders_conql_qlftyr' 
        WHEN STARTS_WITH('hoql', ls.h_tleadtype) = 1 THEN 'homes' 
        WHEN STARTS_WITH('lcql', ls.h_tleadtype) = 1 THEN 'leadcloud' 
        WHEN STARTS_WITH('ptql', ls.h_tleadtype) = 1 THEN 'leadpoint' 
        WHEN STARTS_WITH('ltql', ls.h_tleadtype) = 1 THEN 'lendingtree' 
        WHEN STARTS_WITH('lbql', ls.h_tleadtype) = 1 THEN 'lowermybills' 
        WHEN STARTS_WITH('mfql', ls.h_tleadtype) = 1 THEN 'bills_qlbl_mediaforce' 
        WHEN STARTS_WITH('nmql', ls.h_tleadtype) = 1 THEN 'netmoneywizard' 
        WHEN STARTS_WITH('quizzle', ls.h_tleadtype) = 1 THEN 'quizzle' 
        WHEN STARTS_WITH('acql', ls.h_tleadtype) = 1 THEN 'ratemarketplace_saxumpartners' 
        WHEN STARTS_WITH('rgfql', ls.h_tleadtype) = 1 THEN 'reallygreatrate' 
        WHEN STARTS_WITH('rgql', ls.h_tleadtype) = 1 THEN 'reallygreatrate' 
        WHEN STARTS_WITH('rmql', ls.h_tleadtype) = 1 THEN 'revimedia' 
        WHEN STARTS_WITH('saxqlpl', ls.h_tleadtype) = 1 THEN 'ratemarketplace_saxumpartners'
        WHEN STARTS_WITH('lwql', ls.h_tleadtype) = 1 THEN 'smartquote' 
        WHEN STARTS_WITH('tlnqlp', ls.h_tleadtype) = 1 THEN 'thelendersnetwork'
        ELSE 'other'
        END AS marketing_partner,
    SUBSTRING(ls.h_temail, email_at_index, LENGTH(ls.h_temail)) AS clientemaildomain,
    -- SUBSTRING(ls.h_temail, email_at_index, LENGTH(ls.h_temail)) AS clientemailtopleveldomain,
    SUBSTRING(
            SUBSTRING(ls.h_temail, email_at_index, LENGTH(ls.h_temail)),
            LOCATE('.',  SUBSTRING(ls.h_temail, email_at_index, LENGTH(ls.h_temail))),
            LENGTH (  SUBSTRING(ls.h_temail, email_at_index, LENGTH(ls.h_temail)) )
     ) AS clientemailtopleveldomain,
    CASE 
        WHEN ls.LeadCreateHour = -1 THEN 'missing'
        WHEN ls.LeadCreateHour BETWEEN 0 AND 3 THEN 'weemorning'
        WHEN ls.LeadCreateHour BETWEEN 4 AND 7 THEN 'earlymorning'
        WHEN ls.LeadCreateHour BETWEEN 8 AND 11 THEN 'morning'
        WHEN ls.LeadCreateHour = 12 THEN 'lunchtime'
        WHEN ls.LeadCreateHour BETWEEN 13 AND 15 THEN 'afternoon'
        WHEN ls.LeadCreateHour BETWEEN 16 AND 19 THEN 'evening'
        WHEN ls.LeadCreateHour > 20 'night'
        ELSE 'unkonwn'
        END AS leadtimebucket,
    CASE 
        WHEN ls.LeadCreateHour = -1 THEN 'missing'
        WHEN ls.LeadCreateHour BETWEEN 0 AND 3 THEN 'weemorning'
        WHEN ls.LeadCreateHour BETWEEN 4 AND 7 THEN 'earlymorning'
        WHEN ls.LeadCreateHour BETWEEN 8 AND 11 THEN 'morning'
        WHEN ls.LeadCreateHour = 12 THEN 'lunchtime'
        WHEN ls.LeadCreateHour BETWEEN 13 AND 15 THEN 'afternoon'
        WHEN ls.LeadCreateHour BETWEEN 16 AND 19 THEN 'evening'
        WHEN ls.LeadCreateHour > 20 'night'
        ELSE 'unkonwn'
        END AS leadtimebucket,


    ls.* 
     
FROM lead_allocation.leadsubmissions_vw ls