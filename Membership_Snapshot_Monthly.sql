WITH
-- Base: park x month from revenue, sum pre-calculated potentials split by booking channel
parks_base AS (
    SELECT
          DATE_TRUNC('MONTH', fr.SK_DATE_RECORD)  AS MONTH_START
        , fr.SK_LOCATION
        -- Bucketing all potentials not sold online to 'In Store' for consistency
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED <> 'Online Sales' THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_INPARK
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED =  'Online Sales' THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_ONLINE
        , SUM(fr.POTENTIALS) AS POTENTIALS_TOTAL
    FROM PROD_EDW2_SHARE_GOLD_DB.CNS.TBL_FACTREVENUE fr
    LEFT JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMBOOKING db
        ON db.SK_BOOKING = fr.SK_BOOKING
    GROUP BY 1, 2
),
-- Active member count at start of each month per park
-- A member is active if they joined before month-start and their termination date has not yet passed
mbr_active AS (
    SELECT
          pb.MONTH_START
        , f.SK_LOCATION
        , COUNT(DISTINCT f.SK_TICKET) AS ACTIVE_MEMBERS
    FROM parks_base pb
    JOIN SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS f
        ON  f.SK_LOCATION      = pb.SK_LOCATION
        AND f.DATE_JOIN        < pb.MONTH_START
        AND (f.DATE_TERMINATION IS NULL OR f.DATE_TERMINATION >= pb.MONTH_START)
    GROUP BY 1, 2
),
-- New members added per park per month (by join date), split by booking channel
-- Includes all tickets with a join date — genuine new sales AND upgrade child tickets
-- CONV_TYPE values: 'In Store', 'Online Sales', 'Data Import', 'Venue Manager', NULL
mbr_new AS (
    SELECT
          DATE_TRUNC('MONTH', DATE_JOIN)                                           AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT CASE WHEN CONV_TYPE <> 'Online Sales' THEN TICKETID END)  AS NEW_MEMBERS_INPARK
        , COUNT(DISTINCT CASE WHEN CONV_TYPE =  'Online Sales' THEN TICKETID END)  AS NEW_MEMBERS_ONLINE
        , COUNT(DISTINCT TICKETID)                                                 AS NEW_MEMBERS_TOTAL
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE DATE_JOIN IS NOT NULL
    GROUP BY 1, 2
),
-- Upgrades per park per month (by upgrade date)
mbr_upgrades AS (
    SELECT
          DATE_TRUNC('MONTH', DATE_UPGRADE)                                         AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT TICKETID)                                                  AS UPGRADES_TOTAL
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE CANCEL_REASON = 'Upgraded'
    GROUP BY 1, 2
),
-- Churn broken out by reason, per park per month; excludes upgrades
-- BUG FIX: Voluntary includes ('Cancel Requested', 'Refund', 'Cancel Assumed') and the legacy 'Term Roller'
mbr_churn AS (
    SELECT
          DATE_TRUNC('MONTH', DATE_TERMINATION)                                     AS MONTH_START
        , SK_LOCATION
        -- Voluntary: member-initiated cancels; all non-payment reasons bucketed here so totals reconcile
        , SUM(CASE WHEN CANCEL_REASON NOT IN ('Payment Issue', 'Lapsed')
                    AND CANCEL_REASON IS NOT NULL THEN 1 ELSE 0 END)               AS CHURN_VOLUNTARY
        -- Involuntary: billing failure or lapse due to non-payment
        , SUM(CASE WHEN CANCEL_REASON IN ('Payment Issue', 'Lapsed') THEN 1 ELSE 0 END) AS CHURN_INVOLUNTARY
        , COUNT(*)                                                                 AS CHURN_TOTAL
        -- Cancelled within 33 days of join (before a failed recharge becomes a missed payment)
        , SUM(CASE WHEN CANCEL_DAYS <= 33 THEN 1 ELSE 0 END)                       AS CHURN_FIRST_MONTH
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE DATE_TERMINATION IS NOT NULL
      AND CANCEL_REASON <> 'Upgraded'
    GROUP BY 1, 2
),
-- Reactivated members per park per month
-- A reactivation is any event where SK_EVENTTYPE = 6 ('Reactivated') in FACTMEMBERSHIPPASSEVENTS
mbr_reactivated AS (
    SELECT
          DATE_TRUNC('MONTH', fm.SK_DATE) AS MONTH_START
        , fm.SK_LOCATION
        , COUNT(DISTINCT fm.SK_TICKET) AS MEMBERS_REACTIVATED
    FROM PROD_EDW2_SHARE_GOLD_DB.DW.FACTMEMBERSHIPPASSEVENTS fm
    JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMMEMBERSHIPPASSEVENT dm
        USING (SK_EVENTTYPE)
    WHERE dm.EVENTTYPE = 'Reactivated'
    GROUP BY 1, 2
),
-- Parent add-on memberships sold per park per month Identified by PRODUCT_NAME containing 'parent'
-- PARENT_ADDONS_UNDER18 uses jumper customer birthdate to flag likely abuse (parent add-on sold to a minor)
mbr_parent_addon AS (
    SELECT
          DATE_TRUNC('MONTH', me.DATE_JOIN)                                         AS MONTH_START
        , me.SK_LOCATION
        , COUNT(DISTINCT me.TICKETID)                                               AS PARENT_ADDONS
        , COUNT(DISTINCT CASE WHEN DATEDIFF(year, dc.BIRTHDATE, me.DATE_JOIN) < 18
                               AND DATEDIFF(year, dc.BIRTHDATE, me.DATE_JOIN) > 0
                              THEN me.TICKETID END)                                 AS PARENT_ADDONS_UNDER18
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS me
    LEFT JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMCUSTOMER dc
        ON  dc.SK_CUSTOMER     = me.SK_JUMPERCUSTOMER
        AND dc.DWISCURRENTFLAG = 1
    WHERE me.DATE_JOIN IS NOT NULL
      AND me.PRODUCT_NAME ILIKE '%parent%'
    GROUP BY 1, 2
),
-- Member OSAT survey responses per park per month; filtered to membership and the overall satisfaction question
-- 1 = Highly Dissatisfied, 5 = Highly Satisfied
mbr_osat AS (
    SELECT
          DATE_TRUNC('MONTH', fr.SK_DATE_SURVEY)                                    AS MONTH_START
        , fr.SK_LOCATION
        , COUNT(DISTINCT fr.SK_SURVEY)                                              AS OSAT_MEMBER_COUNT
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '5' THEN fr.SK_SURVEY END)  AS OSAT_5
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '4' THEN fr.SK_SURVEY END)  AS OSAT_4
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '3' THEN fr.SK_SURVEY END)  AS OSAT_3
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '2' THEN fr.SK_SURVEY END)  AS OSAT_2
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '1' THEN fr.SK_SURVEY END)  AS OSAT_1
    FROM PROD_EDW2_SHARE_GOLD_DB.CNS.TBL_FACTSURVEYRESPONSE fr
    JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMSATISFACTIONSURVEY ds
        ON  ds.SK_SURVEY      = fr.SK_SURVEY
        AND ds.MEMBERSHIPFLAG = 'Yes'
    JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMSURVEYQUESTIONSANSWER qa
        ON  qa.SK_SURVEYQA    = fr.SK_SURVEYQA
        AND qa.QUESTIONPROMPT ILIKE '%overall satisfaction%'
        AND qa.NUMERICCODE   <> '99'
    GROUP BY 1, 2
)

SELECT
      pb.MONTH_START
    , dl.LOCATIONID
    , dl.BUSINESSGROUP AS BUSINESS_GROUP
    , am.ACTIVE_MEMBERS AS MEMBERS_ACTIVE
    , pb.POTENTIALS_TOTAL
    , pb.POTENTIALS_INPARK
    , pb.POTENTIALS_ONLINE
    , nm.NEW_MEMBERS_TOTAL
    , nm.NEW_MEMBERS_INPARK
    , nm.NEW_MEMBERS_ONLINE
    , u.UPGRADES_TOTAL
    -- True new sales: new members minus upgrade child tickets (child ticket joins in same month as upgrade)
    , (nm.NEW_MEMBERS_TOTAL - u.UPGRADES_TOTAL) AS NEW_MEMBERS_EXCL_UPGRADES
    , pa.PARENT_ADDONS AS NEW_MEMBERS_PARENT_ADDONS
    , pa.PARENT_ADDONS_UNDER18
    , r.MEMBERS_REACTIVATED
    , c.CHURN_VOLUNTARY
    , c.CHURN_INVOLUNTARY
    , c.CHURN_TOTAL
    -- NULL until 33 days after month-end (window before a failed recharge becomes a missed payment)
    , CASE WHEN CURRENT_DATE >= DATEADD(DAY, 33, LAST_DAY(pb.MONTH_START))
           THEN c.CHURN_FIRST_MONTH
           ELSE NULL
      END  AS CHURN_FIRST_MONTH
    -- NOTE: A ±1 residual is expected in some months due to members whose DATE_JOIN = DATE_TERMINATION (same-day edge case) 
    , (nm.NEW_MEMBERS_TOTAL
        - u.UPGRADES_TOTAL
        - c.CHURN_TOTAL) AS MEMBERS_NET_CHANGE
    , o.OSAT_MEMBER_COUNT
    , o.OSAT_5
    , o.OSAT_4
    , o.OSAT_3
    , o.OSAT_2
    , o.OSAT_1
FROM parks_base pb
LEFT JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMLOCATION dl
    ON  dl.SK_LOCATION     = pb.SK_LOCATION
    AND dl.DWISCURRENTFLAG = 1
LEFT JOIN mbr_active am
    ON  am.SK_LOCATION = pb.SK_LOCATION
    AND am.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_new nm
    ON  nm.SK_LOCATION = pb.SK_LOCATION
    AND nm.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_upgrades u
    ON  u.SK_LOCATION = pb.SK_LOCATION
    AND u.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_reactivated r
    ON  r.SK_LOCATION = pb.SK_LOCATION
    AND r.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_churn c
    ON  c.SK_LOCATION = pb.SK_LOCATION
    AND c.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_parent_addon pa
    ON  pa.SK_LOCATION = pb.SK_LOCATION
    AND pa.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_osat o
    ON  o.SK_LOCATION = pb.SK_LOCATION
    AND o.MONTH_START = pb.MONTH_START
ORDER BY pb.MONTH_START DESC, dl.LOCATIONID
;
