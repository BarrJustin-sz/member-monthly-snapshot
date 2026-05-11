
-- Each row in this output represents a calendar month's performance by park, labeled by the LAST DAY of that month
-- (e.g., the row dated 2026-04-30 summarizes active members at end of April and all activity that occurred during April 2026).
-- Internally, CTEs shift MONTH_START forward by one month (+1) so April events join to the 2026-05-01 spine row,
-- and SK_DATE_RECORD is then output as DATEADD(DAY, -1, MONTH_START) to relabel it as 2026-04-30.
WITH
-- Maps all historical SK_LOCATIONs for the target park to the current surrogate key.
-- Prevents duplicate months when a new SCD2 row is minted for a park mid-month.
location_map AS (
    SELECT
          h.SK_LOCATION  AS HIST_SK
        , c.SK_LOCATION  AS CURRENT_SK
    FROM GOLD_DB.CNS.TBL_DIMLOCATION h
    JOIN GOLD_DB.CNS.TBL_DIMLOCATION c
        ON  c.LOCATIONID       = h.LOCATIONID
        AND c.DWISCURRENTFLAG  = 1
    WHERE h.COUNTRY IN ('US', 'CA', 'Hong Kong')
),
-- Pre-normalize TBL_FACTMEMBERSHIP_LASTEVENTS to the current SK for this park.
-- All downstream membership CTEs use this instead of the raw fact table.
mbr_facts_base AS (
    SELECT f.*, lm.CURRENT_SK AS SK_LOC_NORM
    FROM GOLD_DB.CNS.TBL_FACTMEMBERSHIP_LASTEVENTS f
    JOIN location_map lm ON lm.HIST_SK = f.SK_LOCATION
),
-- Base: park x month from revenue, sum pre-calculated potentials split by booking channel
parks_base AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fr.SK_DATE_RECORD))  AS MONTH_START
        , lm.CURRENT_SK AS SK_LOCATION
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED NOT IN ('Online Sales', 'Venue Manager') THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_INPARK
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED IN ('Online Sales', 'Venue Manager')    THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_ONLINE
        , SUM(fr.POTENTIALS) AS POTENTIALS_TOTAL
    FROM GOLD_DB.CNS.TBL_FACTREVENUE fr
    JOIN location_map lm ON lm.HIST_SK = fr.SK_LOCATION
    LEFT JOIN GOLD_DB.DW.DIMBOOKING db
        ON db.SK_BOOKING = fr.SK_BOOKING
    GROUP BY 1, 2
),
-- Active member count at end of the reporting month (= start of the following month internally)
-- A member is active if they joined before the internal MONTH_START and their termination date has not yet passed
mbr_active AS (
    SELECT
          pb.MONTH_START
        , pb.SK_LOCATION
        , COUNT(DISTINCT f.SK_TICKET) AS ACTIVE_MEMBERS
    FROM parks_base pb
    JOIN mbr_facts_base f
        ON  f.SK_LOC_NORM         = pb.SK_LOCATION
        AND f.SK_DATE_JOIN        < pb.MONTH_START
        AND (f.SK_DATE_TERMINATION IS NULL OR f.SK_DATE_TERMINATION >= pb.MONTH_START)
    GROUP BY 1, 2
),
-- New members added per park per month (by join date), split by booking channel
-- Includes all tickets with a join date — genuine new sales AND upgrade child tickets
-- CONV_TYPE values: 'In Store', 'Online Sales', 'Data Import', 'Venue Manager', NULL
mbr_new AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', f.SK_DATE_JOIN))                        AS MONTH_START
        , f.SK_LOC_NORM                                                                 AS SK_LOCATION
        , COUNT(DISTINCT CASE WHEN f.CONV_TYPE NOT IN ('Online Sales', 'Venue Manager') THEN TICKETID END)  AS NEW_MEMBERS_INPARK
        , COUNT(DISTINCT CASE WHEN f.CONV_TYPE IN ('Online Sales', 'Venue Manager')     THEN TICKETID END)  AS NEW_MEMBERS_ONLINE
        , COUNT(DISTINCT t.TICKETID)                                                    AS NEW_MEMBERS_TOTAL
    FROM mbr_facts_base f
    JOIN GOLD_DB.DW.DIMTICKET t USING(SK_TICKET)
    WHERE f.SK_DATE_JOIN IS NOT NULL
    GROUP BY 1, 2
),
-- Upgrades per park per month (by upgrade date)
mbr_upgrades AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', f.SK_DATE_UPGRADE))    AS MONTH_START
        , f.SK_LOC_NORM                             AS SK_LOCATION
        , COUNT(DISTINCT t.TICKETID)                AS UPGRADES_TOTAL
    FROM mbr_facts_base f
    JOIN GOLD_DB.DW.DIMTICKET t USING(SK_TICKET)
    WHERE f.CANCEL_REASON = 'Upgraded'
    GROUP BY 1, 2
),
-- Churn broken out by reason, per park per month (by termination month); excludes upgrades
-- BUG FIX: Voluntary includes ('Cancel Requested', 'Refund', 'Cancel Assumed') and the legacy 'Term Roller'
mbr_churn AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', f.SK_DATE_TERMINATION))                       AS MONTH_START
        , f.SK_LOC_NORM                                                                        AS SK_LOCATION
        -- Voluntary: member-initiated cancels; all non-payment reasons bucketed here so totals reconcile
        , SUM(CASE WHEN f.CANCEL_REASON NOT IN ('Payment Issue', 'Lapsed')
                    AND f.CANCEL_REASON IS NOT NULL THEN 1 ELSE 0 END)                         AS CHURN_VOLUNTARY
        -- Involuntary: billing failure or lapse due to non-payment
        , SUM(CASE WHEN f.CANCEL_REASON IN ('Payment Issue', 'Lapsed') THEN 1 ELSE 0 END)      AS CHURN_INVOLUNTARY
        , COUNT(*)                                                                             AS CHURN_TOTAL
    FROM mbr_facts_base f
    WHERE f.SK_DATE_TERMINATION IS NOT NULL
      AND f.CANCEL_REASON <> 'Upgraded'
    GROUP BY 1, 2
),
-- First-month churn attributed to the JOIN month, not the termination month.
-- Counts members who churned within 33 days of joining, bucketed by when they signed up.
-- A Feb 21 joiner who cancels Mar 5 counts in February here.
mbr_churn_first_month AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', f.SK_DATE_JOIN))   AS MONTH_START
        , f.SK_LOC_NORM                                          AS SK_LOCATION
        , COUNT(*)                                               AS CHURN_FIRST_MONTH
    FROM mbr_facts_base f
    WHERE f.SK_DATE_TERMINATION IS NOT NULL
      AND f.CANCEL_REASON <> 'Upgraded'
      AND f.CANCEL_DAYS <= 33
    GROUP BY 1, 2
),
-- Reactivated members per park per month
-- A reactivation is any event where SK_EVENTTYPE = 6 ('Reactivated') in FACTMEMBERSHIPPASSEVENTS
mbr_reactivated AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fm.SK_DATE)) AS MONTH_START
        , lm.CURRENT_SK                   AS SK_LOCATION
        , COUNT(DISTINCT fm.SK_TICKET)    AS MEMBERS_REACTIVATED
    FROM GOLD_DB.CNS.TBL_FACTMEMBERSHIPPASSEVENTS fm
    JOIN location_map lm ON lm.HIST_SK = fm.SK_LOCATION
    JOIN GOLD_DB.DW.DIMMEMBERSHIPPASSEVENT dm
        USING (SK_EVENTTYPE)
    WHERE dm.EVENTTYPE = 'Reactivated'
    GROUP BY 1, 2
),
-- Parent add-on memberships sold per park per month Identified by PRODUCT_NAME containing 'parent'
-- PARENT_ADDONS_UNDER18 uses jumper customer birthdate to flag likely abuse (parent add-on sold to a minor)
mbr_parent_addon AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', me.SK_DATE_JOIN))                      AS MONTH_START
        , me.SK_LOC_NORM                                                                AS SK_LOCATION
        , COUNT(DISTINCT t.TICKETID)                                                    AS PARENT_ADDONS
        , COUNT(DISTINCT CASE WHEN DATEDIFF(year, dc.BIRTHDATE, me.SK_DATE_JOIN) < 18
                               AND DATEDIFF(year, dc.BIRTHDATE, me.SK_DATE_JOIN) > 0
                              THEN t.TICKETID END)                                      AS PARENT_ADDONS_UNDER18
    FROM mbr_facts_base me
    JOIN GOLD_DB.DW.DIMTICKET t USING(SK_TICKET)
    JOIN GOLD_DB.CNS.TBL_DIMPRODUCT p USING(SK_PRODUCT)
    LEFT JOIN GOLD_DB.DW.DIMCUSTOMER dc
        ON  dc.SK_CUSTOMER     = me.SK_JUMPERCUSTOMER
        AND dc.DWISCURRENTFLAG = 1
    WHERE me.SK_DATE_JOIN IS NOT NULL
      AND CONCAT(NVL(p.PRODUCTNAME,''), NVL(p.PARENTPRODUCTNAME,''))::STRING ILIKE '%parent%'
    GROUP BY 1, 2
),
-- Member OSAT survey responses per park per month; filtered to membership and the overall satisfaction question
-- 1 = Highly Dissatisfied, 5 = Highly Satisfied
mbr_osat AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fr.SK_DATE_SURVEY))               AS MONTH_START
        , lm.CURRENT_SK                                                             AS SK_LOCATION
        , COUNT(DISTINCT fr.SK_SURVEY)                                              AS OSAT_MEMBER_COUNT
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '5' THEN fr.SK_SURVEY END)  AS OSAT_5
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '4' THEN fr.SK_SURVEY END)  AS OSAT_4
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '3' THEN fr.SK_SURVEY END)  AS OSAT_3
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '2' THEN fr.SK_SURVEY END)  AS OSAT_2
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '1' THEN fr.SK_SURVEY END)  AS OSAT_1
    FROM GOLD_DB.CNS.TBL_FACTSURVEYRESPONSE fr
    JOIN location_map lm ON lm.HIST_SK = fr.SK_LOCATION
    JOIN GOLD_DB.DW.DIMSATISFACTIONSURVEY ds
        ON  ds.SK_SURVEY      = fr.SK_SURVEY
        AND ds.MEMBERSHIPFLAG = 'Yes'
    JOIN GOLD_DB.DW.DIMSURVEYQUESTIONSANSWER qa
        ON  qa.SK_SURVEYQA    = fr.SK_SURVEYQA
        AND qa.QUESTIONPROMPT ILIKE '%overall satisfaction%'
        AND qa.NUMERICCODE   <> '99'
    GROUP BY 1, 2
)

SELECT
      DATEADD('DAY', -1, pb.MONTH_START)::DATE AS SK_DATE_RECORD  --> Last day of the reporting month; joins to GOLD_DB.DW.DIMDATE in AAS
    , pb.SK_LOCATION::INT AS SK_LOCATION                        --> To join with GOLD_DB.CNS.TBL_DIMLOCATION in AAS (always current SK)
    , am.ACTIVE_MEMBERS::FLOAT AS MEMBERS_ACTIVE
    , pb.POTENTIALS_TOTAL::FLOAT AS POTENTIALS_TOTAL
    , pb.POTENTIALS_INPARK::FLOAT AS POTENTIALS_INPARK
    , pb.POTENTIALS_ONLINE::FLOAT AS POTENTIALS_ONLINE
    , nm.NEW_MEMBERS_TOTAL::FLOAT AS NEW_MEMBERS_TOTAL
    , nm.NEW_MEMBERS_INPARK::FLOAT AS NEW_MEMBERS_INPARK
    , nm.NEW_MEMBERS_ONLINE::FLOAT AS NEW_MEMBERS_ONLINE
    , u.UPGRADES_TOTAL::FLOAT AS UPGRADES_TOTAL
    -- True new sales: new members minus upgrade child tickets (child ticket joins in same month as upgrade)
    , (NVL(nm.NEW_MEMBERS_TOTAL,0) - NVL(u.UPGRADES_TOTAL,0))::FLOAT AS NEW_MEMBERS_EXCL_UPGRADES
    , pa.PARENT_ADDONS::FLOAT AS NEW_MEMBERS_PARENT_ADDONS
    , pa.PARENT_ADDONS_UNDER18::FLOAT AS PARENT_ADDONS_UNDER18
    , r.MEMBERS_REACTIVATED::FLOAT AS MEMBERS_REACTIVATED
    , c.CHURN_VOLUNTARY::FLOAT AS CHURN_VOLUNTARY
    , c.CHURN_INVOLUNTARY::FLOAT AS CHURN_INVOLUNTARY
    , c.CHURN_TOTAL::FLOAT AS CHURN_TOTAL
    -- NULL until 33 days after the end of the join cohort month (one month prior to the row label)
    , CASE WHEN CURRENT_DATE >= DATEADD(DAY, 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START)))
           THEN cfm.CHURN_FIRST_MONTH
           ELSE NULL
      END::FLOAT AS CHURN_FIRST_MONTH
    -- Reactivations are excluded: TBL_FACTMEMBERSHIP_LASTEVENTS stores last-event-per-ticket,
    -- so reactivated members already appear in the active count (their SK_DATE_TERMINATION is
    -- updated to a future date) and were never subtracted via CHURN_TOTAL. Adding REACTIVATED
    -- here would double-count them. Formula reconciles to actual active delta within ~20 members/month.
    , (NVL(nm.NEW_MEMBERS_TOTAL,0)
        - NVL(u.UPGRADES_TOTAL,0)
        - NVL(c.CHURN_TOTAL,0))::FLOAT AS MEMBERS_NET_CHANGE
    , o.OSAT_MEMBER_COUNT::FLOAT AS OSAT_MEMBER_COUNT
    , o.OSAT_5::FLOAT AS OSAT_5
    , o.OSAT_4::FLOAT AS OSAT_4
    , o.OSAT_3::FLOAT AS OSAT_3
    , o.OSAT_2::FLOAT AS OSAT_2
    , o.OSAT_1::FLOAT AS OSAT_1
FROM parks_base pb
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
LEFT JOIN mbr_churn_first_month cfm
    ON  cfm.SK_LOCATION = pb.SK_LOCATION
    AND cfm.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_parent_addon pa
    ON  pa.SK_LOCATION = pb.SK_LOCATION
    AND pa.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_osat o
    ON  o.SK_LOCATION = pb.SK_LOCATION
    AND o.MONTH_START = pb.MONTH_START
LEFT JOIN GOLD_DB.CNS.TBL_DIMLOCATION dl
    ON  dl.SK_LOCATION    = pb.SK_LOCATION
    AND dl.DWISCURRENTFLAG = 1
-- Only show months where the full calendar month has passed (MONTH_START crosses into the next month)
WHERE pb.MONTH_START <= DATE_TRUNC('MONTH', CURRENT_DATE)
  AND dl.LOCATIONID = 'Apex, NC - 151'
ORDER BY pb.MONTH_START DESC, dl.LOCATIONID
;
