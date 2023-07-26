--historical_conversion 
    --LTC_close_rate
    --state_close_rate
    --adproperty_close_rate
--campaign
    --escalationcount
    --preesc_previouscallcount
    --preesc_previoustransfercount
--lead 
    --pull from vw    
        --get_leadcreatehour ( pull from lead_submissions_vw) 
        --get_clientemaildomain
        --get_clientemailtopleveldomain
        --get_leadtimebucket
        --get_loanpurpose
        --get_leadtypebucket
        --get_sweepstakesflag
        --marketingpartner
--call 
    --get_hourofdial
    --get_dialtimebucket
    
    
CREATE VIEW lead_allocation.leadsubmissions_vw AS 

select 
    leads.* , 
    CASE 
        WHEN leads.TimeZone = 'EST' THEN  0 
        WHEN leads.TimeZone = 'CST' THEN  -1 
        WHEN leads.TimeZone = 'MST' THEN  -2
        WHEN leads.TimeZone = 'PST' THEN  -3
        WHEN leads.TimeZone = 'Alaska' THEN  -4 
        WHEN leads.TimeZone = 'Hawaii' THEN  -5
        WHEN leads.TimeZone = 'Other' THEN  -6
        END AS localhourconversion
FROM 
(
    select
        ls.loannumber, 
        lower(ls.h_sloan) as h_sloan,
        lower(ls.h_tleadtype) as h_tleadtype,
        lower(ls.h_tvenleadid) as h_tvenleadid,
        lower(ls.h_tadproperty) as h_tadproperty, 
        ls.h_sstate, 
        ls.h_tzip, 
        ls.h_tcity, 
        ls.leadcreatedate,
        TIMESTAMPDIFF (SECOND,  "leadcreatedate", current_timestamp() ) / 60 / 60 / 24.00  AS leadagedays,
        HOUR(ls.leadcreatedate) AS LeadCreateHour,
        CASE 
            WHEN h_sstate IN ('WA', 'OR', 'CA', 'NV') THEN 'PST'
            WHEN h_sstate IN ('MT', 'WY', 'UT', 'CO', 'AZ', 'NM', 'ND', 'SD', 'NE', 'KS', 'TX', 'ID') THEN 'MST' 
            WHEN h_sstate IN ('MN', 'IA', 'MO', 'AR', 'OK', 'MS', 'AL', 'TN', 'IL', 'WI', 'LA') THEN 'CST'
            WHEN h_sstate IN ( 'ME', 'NH', 'VT', 'MA', 'NY', 'RI', 'CT', 'NJ', 'DE', 'MD', 'PA', 'WV', 'OH', 'MI', 'IN', 'KY', 'VA', 'NC','SC','GA', 'FL', 'DC') THEN 'EST'
            WHEN h_sstate IN ( 'HI') THEN 'Hawaii'
            WHEN h_sstate IN ( 'AK') THEN 'Alaska'
            ELSE 'Other'
            END AS TimeZone,
        ls.h_temail,
        LOCATE('@', ls.h_temail)	email_at_index
    from lead_allocation.leadsubmissions ls
) leads 