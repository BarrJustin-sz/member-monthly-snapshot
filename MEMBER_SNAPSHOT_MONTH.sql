

WITH
-- Maps all historical SK_LOCATIONs for the target park to the current surrogate key.
-- Prevents duplicate months when a new SCD2 row is minted for a park mid-month.
location_map AS (
    SELECT
          SK_LOCATION,
          SK_LOCATION_ACTIVE
    FROM GOLD_DB.CNS.TBL_DIMLOCATION
    WHERE COUNTRY IN ('US', 'CA', 'Hong Kong')
),
-- Pre-normalize TBL_FACTMEMBERSHIP_LASTEVENTS to the current SK for this park.
-- All downstream membership CTEs use this instead of the raw fact table.
mbr_facts_base AS (
    SELECT       
      f.SK_LOCATION
    , lm.SK_LOCATION_ACTIVE
    , f.SK_PRODUCT
    , f.SK_BOOKING
    , f.SK_TICKET
    , f.SK_BOOKINGCREATEDBYEMPLOYEE
    , f.SK_HOUSEHOLD
    , f.SK_JUMPERCUSTOMER
    , f.SK_PURCHASINGCUSTOMER
    , f.SK_DATE_JOIN
    , f.SK_DATE_LAST_CHECKIN
    , f.SK_DATE_UPGRADE
    , f.SK_DATE_LAST_REFUND
    , f.SK_DATE_CANCEL
    , f.SK_DATE_TERMINATION
    , f.SK_DATE_RECURR_LAST_PAY
    , f.SK_DATE_RECURR_NEXT_PAY
    , f.BOOKINGITEMID
    , f.CONV_TYPE
    , f.STATUS_LAST
    , f.STATUS_ROLLER
    , f.CANCEL_REASON
    , f.CUSTOMER_JUMPER
    , f.CUSTOMER_PURCHASE
    , f.STATUS_PROJ
    , f.STATUS_ACTIVE
    , f.CHECKIN_COUNT
    , f.PAY_INITIAL
    , f.RECURR_AVG_DUES
    , f.RECURR_PAY_COUNT
    , f.REFUND_COUNT
    , f.CANCEL_DAYS
    FROM GOLD_DB.CNS.TBL_FACTMEMBERSHIP_LASTEVENTS f
    JOIN location_map lm USING(SK_LOCATION)
),
-- Base: park x month from revenue, sum pre-calculated potentials split by booking channel
-- Each row in this output represents a calendar month's performance by park, labeled by the LAST DAY of that month (e.g., the row dated 2026-04-30 summarizes active members at end of April and all activity that occurred during April 2026).
-- DATE CONVENTION: Event dates are bucketed as DATEADD('MONTH', 1, DATE_TRUNC('MONTH', <event_date>)) to shift MONTH_START forward by one month (+1) and the final select then outputs as DATEADD(DAY, -1, MONTH_START) to relabel it as the last day of the month. (e.g., internal May 1 → output April 30).
parks_base AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fr.SK_DATE_RECORD))                                                           AS MONTH_START
        , lm.SK_LOCATION_ACTIVE                                                                                                 AS SK_LOCATION
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED NOT IN ('Online Sales', 'Venue Manager') THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_INPARK
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED IN ('Online Sales', 'Venue Manager')    THEN fr.POTENTIALS ELSE 0 END)   AS POTENTIALS_ONLINE
        , SUM(fr.POTENTIALS)                                                                                                    AS POTENTIALS_TOTAL
    FROM GOLD_DB.CNS.TBL_FACTREVENUE fr
    JOIN location_map lm USING(SK_LOCATION)
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
        ON  f.SK_LOCATION_ACTIVE  = pb.SK_LOCATION
        AND f.SK_DATE_JOIN        < pb.MONTH_START
        AND (f.SK_DATE_TERMINATION IS NULL OR f.SK_DATE_TERMINATION >= pb.MONTH_START)
    GROUP BY 1, 2
),
-- New members added per park per month (by join date), split by booking channel
-- Includes all tickets with a join date — genuine new sales AND upgrade child tickets
-- CONV_TYPE values: 'In Store', 'Online Sales', 'Data Import', 'Venue Manager', NULL
mbr_new AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', SK_DATE_JOIN))                                          AS MONTH_START
        , SK_LOCATION_ACTIVE                                                                              AS SK_LOCATION
        , COUNT(DISTINCT CASE WHEN CONV_TYPE NOT IN ('Online Sales', 'Venue Manager') THEN SK_TICKET END) AS NEW_MEMBERS_INPARK
        , COUNT(DISTINCT CASE WHEN CONV_TYPE IN ('Online Sales', 'Venue Manager')     THEN SK_TICKET END) AS NEW_MEMBERS_ONLINE
        , COUNT(DISTINCT SK_TICKET)                                                                       AS NEW_MEMBERS_TOTAL
    FROM mbr_facts_base f
    WHERE SK_DATE_JOIN IS NOT NULL
    GROUP BY 1, 2
),
-- Upgrades per park per month (by upgrade date)
mbr_upgrades AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', SK_DATE_UPGRADE))       AS MONTH_START
        , SK_LOCATION_ACTIVE                                              AS SK_LOCATION
        , COUNT(DISTINCT SK_TICKET)                                       AS UPGRADES_TOTAL
    FROM mbr_facts_base 
    WHERE CANCEL_REASON = 'Upgraded'
    GROUP BY 1, 2
),
-- Reactivated members per park per month
-- A reactivation is any event where SK_EVENTTYPE = 6 ('Reactivated') in FACTMEMBERSHIPPASSEVENTS
mbr_reactivated AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fm.SK_DATE))          AS MONTH_START
        , lm.SK_LOCATION_ACTIVE                                         AS SK_LOCATION
        , COUNT(DISTINCT fm.SK_TICKET)                                  AS MEMBERS_REACTIVATED
    FROM GOLD_DB.CNS.TBL_FACTMEMBERSHIPPASSEVENTS fm
    JOIN location_map lm USING(SK_LOCATION)
    JOIN GOLD_DB.DW.DIMMEMBERSHIPPASSEVENT dm
        USING (SK_EVENTTYPE)
    WHERE dm.EVENTTYPE = 'Reactivated'
    GROUP BY 1, 2
),
-- Parent add-on memberships sold per park per month Identified by PRODUCT_NAME containing 'parent'
-- PARENT_ADDONS_UNDER18 uses jumper customer birthdate to flag likely abuse (parent add-on sold to a minor)
mbr_parent_addon AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', me.SK_DATE_JOIN))                     AS MONTH_START
        , me.SK_LOCATION_ACTIVE                                                         AS SK_LOCATION
        , COUNT(DISTINCT me.SK_TICKET)                                                  AS PARENT_ADDONS
        , COUNT(DISTINCT CASE WHEN DATEDIFF(year, dc.BIRTHDATE, me.SK_DATE_JOIN) < 18
                               AND DATEDIFF(year, dc.BIRTHDATE, me.SK_DATE_JOIN) > 0
                              THEN me.SK_TICKET END)                                    AS PARENT_ADDONS_UNDER18
    FROM mbr_facts_base me
    JOIN GOLD_DB.CNS.TBL_DIMPRODUCT p USING(SK_PRODUCT)
    LEFT JOIN GOLD_DB.DW.DIMCUSTOMER dc
        ON  dc.SK_CUSTOMER = me.SK_JUMPERCUSTOMER
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
        , lm.SK_LOCATION_ACTIVE                                                     AS SK_LOCATION
        , COUNT(DISTINCT fr.SK_SURVEY)                                              AS OSAT_MEMBER_COUNT
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '5' THEN fr.SK_SURVEY END)  AS OSAT_5
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '4' THEN fr.SK_SURVEY END)  AS OSAT_4
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '3' THEN fr.SK_SURVEY END)  AS OSAT_3
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '2' THEN fr.SK_SURVEY END)  AS OSAT_2
        , COUNT(DISTINCT CASE WHEN fr.RESPONSENUMERIC = '1' THEN fr.SK_SURVEY END)  AS OSAT_1
    FROM GOLD_DB.CNS.TBL_FACTSURVEYRESPONSE fr
    JOIN location_map lm USING(SK_LOCATION)
    JOIN GOLD_DB.DW.DIMSATISFACTIONSURVEY ds
        ON  ds.SK_SURVEY      = fr.SK_SURVEY
        AND ds.MEMBERSHIPFLAG = 'Yes'
    JOIN GOLD_DB.DW.DIMSURVEYQUESTIONSANSWER qa
        ON  qa.SK_SURVEYQA    = fr.SK_SURVEYQA
        AND qa.QUESTIONPROMPT ILIKE '%overall satisfaction%'
        AND qa.NUMERICCODE   <> '99'
    GROUP BY 1, 2
),
-- Booking-level sock attachment for new-member bookings only.
-- TICKETQUANTITY > 0 excludes Roller's zero-qty revenue split rows and refund/reversal rows.
-- SOCK_QTY_CAPPED: socks capped at membership qty per booking (e.g. 2 members + 3 socks = 2 socks capped).
-- IS_INPARK: 1 if any ticket on the booking was sold in-park (CONV_TYPE not Online/Venue Manager).
sock_booking_bridge AS (
    SELECT
          f.SK_BOOKING
        , NVL(s.SOCK_QTY_RAW, 0)                                                                AS SOCK_QTY_RAW
        , MAX(CASE WHEN f.CONV_TYPE NOT IN ('Online Sales', 'Venue Manager') THEN 1 ELSE 0 END) AS IS_INPARK
    FROM mbr_facts_base f
    LEFT JOIN (
        SELECT
              fr.SK_BOOKING
            , SUM(fr.TICKETQUANTITY)  AS SOCK_QTY_RAW
        FROM GOLD_DB.CNS.TBL_FACTREVENUE fr
        JOIN GOLD_DB.CNS.TBL_DIMPRODUCT p
            ON  p.SK_PRODUCT     = fr.SK_PRODUCT
            AND p.HOLYGRAILGROUP = 'Socks'
        WHERE fr.TICKETQUANTITY > 0
        GROUP BY fr.SK_BOOKING
    ) s ON s.SK_BOOKING = f.SK_BOOKING
    WHERE f.SK_DATE_JOIN IS NOT NULL
    GROUP BY f.SK_BOOKING, s.SOCK_QTY_RAW
),
-- Booking-level sock attach rate rolled up to park x month.
-- SOCKS_W_MBR_CAPPED_INPARK excludes online bookings where socks are optional.
-- Cap is applied at booking level so there cant be more socks sold than memberships if there are multiple products on the same transaction. 
-- Known gap || If there are multiple products on the same transaction, we can't determine if the socks are being attached to the member or ticket.
mbr_socks AS (
    SELECT
          MONTH_START
        , SK_LOCATION
        , SUM(SOCK_QTY_CAPPED)                                                  AS SOCKS_W_MBR_CAPPED
        , SUM(CASE WHEN IS_INPARK = 1 THEN SOCK_QTY_CAPPED ELSE 0 END)         AS SOCKS_W_MBR_CAPPED_INPARK
    FROM (
        SELECT
              DATEADD('MONTH', 1, DATE_TRUNC('MONTH', f.SK_DATE_JOIN))          AS MONTH_START
            , f.SK_LOCATION_ACTIVE                                              AS SK_LOCATION
            , f.SK_BOOKING
            , sb.IS_INPARK
            , LEAST(sb.SOCK_QTY_RAW, COUNT(DISTINCT f.SK_TICKET))              AS SOCK_QTY_CAPPED
        FROM mbr_facts_base f
        JOIN sock_booking_bridge sb ON sb.SK_BOOKING = f.SK_BOOKING
        WHERE f.SK_DATE_JOIN IS NOT NULL
        GROUP BY 1, 2, f.SK_BOOKING, sb.SOCK_QTY_RAW, sb.IS_INPARK
    )
    GROUP BY 1, 2
),
-- Recurring dues collected per park per month, from TBL_FACTREVENUE filtered to TRANSACTIONLOCATION ILIKE '%recurring%' (='Recurring billing') consistent with COMPPARK_AZ logic.
mbr_recurring_collected AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fr.SK_DATE_RECORD))           AS MONTH_START
        , lm.SK_LOCATION_ACTIVE                                                 AS SK_LOCATION
        , SUM(fr.MEMBERSHIP_REVENUE)                                            AS RECURRING_DUES_COLLECTED
    FROM GOLD_DB.CNS.TBL_FACTREVENUE fr
    JOIN location_map lm USING(SK_LOCATION)
    LEFT JOIN GOLD_DB.DW.DIMTRANSACTIONPAYMENTLABEL tpl
        ON tpl.SK_TRANSACTIONPAYMENTLABEL = fr.SK_TRANSACTIONPAYMENTLABEL
    WHERE tpl.TRANSACTIONLOCATION ILIKE '%recurring%'
    GROUP BY 1, 2
),
-- New-member dues collected per park per month (NET of refunds), from TBL_FACTREVENUE NEW = TRANSACTIONLOCATION NOT ILIKE '%recurring%' (initial sale / activation, not recurring billing). consistent with COMPPARK_AZ logic.
-- NEW_DUES_COLLECTED_WITH_REFUNDS: Venue Manager refund/reversal rows are NOT excluded, so they net against sales — matches COMPPARK_AZ COMP_REVENUE_NEW_MEMBERSHIP_WITH_REFUNDS.
-- Bucketed by transaction date, so a refund processed in a later month nets against THAT month, not the original sale month.
mbr_new_collected AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', fr.SK_DATE_RECORD))                                                  AS MONTH_START
        , lm.SK_LOCATION_ACTIVE                                                                                        AS SK_LOCATION
        , SUM(fr.MEMBERSHIP_REVENUE)                                                                                   AS NEW_DUES_COLLECTED_WITH_REFUNDS
    FROM GOLD_DB.CNS.TBL_FACTREVENUE fr
    JOIN location_map lm USING(SK_LOCATION)
    LEFT JOIN GOLD_DB.DW.DIMTRANSACTIONPAYMENTLABEL tpl
        ON tpl.SK_TRANSACTIONPAYMENTLABEL = fr.SK_TRANSACTIONPAYMENTLABEL
    WHERE tpl.TRANSACTIONLOCATION NOT ILIKE '%recurring%'
    GROUP BY 1, 2
),
-- Churn broken out by reason, per park per month (by termination month); excludes upgrades
-- Upgraded tickets are EXCLUDED from the cohort (CANCEL_REASON <> 'Upgraded'). An upgrade mints a new SK_TICKET that re-enters as a fresh join in its own month, so the old upgraded ticket is dropped here rather than counted as retained.
-- BUG FIX: Voluntary includes ('Cancel Requested', 'Refund', 'Cancel Assumed') and the legacy 'Term Roller'
mbr_churned_by_reason AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', SK_DATE_TERMINATION))                           AS MONTH_START
        , SK_LOCATION_ACTIVE                                                                      AS SK_LOCATION
        -- Voluntary: member-initiated cancels; all non-payment reasons bucketed here so totals reconcile
        , SUM(CASE WHEN CANCEL_REASON NOT IN ('Payment Issue', 'Lapsed')
                    AND CANCEL_REASON IS NOT NULL THEN 1 ELSE 0 END)                              AS CHURN_VOLUNTARY
        -- Involuntary: billing failure or lapse due to non-payment
        , SUM(CASE WHEN CANCEL_REASON IN ('Payment Issue', 'Lapsed') THEN 1 ELSE 0 END)           AS CHURN_INVOLUNTARY
        , COUNT(DISTINCT SK_TICKET)                                                               AS CHURN_TOTAL
    FROM mbr_facts_base 
    WHERE SK_DATE_TERMINATION IS NOT NULL
      AND CANCEL_REASON <> 'Upgraded'
    GROUP BY 1, 2
),
-- Average duration (months) of members who CHURNED in the reporting month.
-- DUR_CHURN_WTAVG_DAYS_SUM and DUR_CHURN_WTAVG_MEMBER_COUNT are the numerator/denominator components for wavg — only use when rolling up to region or district. Do NOT use either column as a standalone metric.
-- WHY CHURNED-ONLY: Including all active members in a duration avg is misleading; their tenure is still in progress and grows every month  by staying active, causing the metric to drift down with new members join seasonality and upward as the base ages.
-- Restricting to churned members gives a stable, completed-tenure view: "how long did members last before leaving?"
mbr_churned_avg_duration AS (
    SELECT
          DATEADD('MONTH', 1, DATE_TRUNC('MONTH', f.SK_DATE_TERMINATION))   AS MONTH_START
        , f.SK_LOCATION_ACTIVE                                              AS SK_LOCATION
        , ROUND(AVG(f.CANCEL_DAYS / 30.44), 2)                              AS DUR_CHURN_AVG_MONTHS
        , SUM(f.CANCEL_DAYS)                                                AS DUR_CHURN_WTAVG_DAYS_SUM
        , COUNT(DISTINCT f.SK_TICKET)                                       AS DUR_CHURN_WTAVG_MEMBER_COUNT
    FROM mbr_facts_base f
    WHERE f.SK_DATE_TERMINATION IS NOT NULL
      AND f.CANCEL_REASON <> 'Upgraded'
    GROUP BY 1, 2
),
-- Rolling 12-month average duration of churned members. Pools the trailing 12 months of churn to smooth seasonal spikes — preferred for SPS forecasting.
-- Same churned-only rationale as mbr_churned_avg_duration above; L12M window smooths month-to-month volatility from small churn cohorts.
-- DUR_CHURN_L12M_WTAVG_DAYS_SUM and DUR_CHURN_L12M_WTAVG_MEMBER_COUNT are the numerator/denominator components for wavg — only use these together when rolling up to region or district. Do NOT use either column as a standalone metric.
mbr_churned_avg_duration_l12m AS (
    SELECT
          pb.MONTH_START
        , pb.SK_LOCATION
        , ROUND(AVG(f.CANCEL_DAYS / 30.44), 2)                              AS DUR_CHURN_L12M_AVG_MONTHS
        , SUM(f.CANCEL_DAYS)                                                AS DUR_CHURN_L12M_WTAVG_DAYS_SUM
        , COUNT(DISTINCT f.SK_TICKET)                                       AS DUR_CHURN_L12M_WTAVG_MEMBER_COUNT
    FROM parks_base pb
    JOIN mbr_facts_base f
        ON  f.SK_LOCATION_ACTIVE   = pb.SK_LOCATION
        AND f.SK_DATE_TERMINATION  >= DATEADD('MONTH', -12, pb.MONTH_START)
        AND f.SK_DATE_TERMINATION  <  pb.MONTH_START
        AND f.SK_DATE_TERMINATION  IS NOT NULL
        AND f.CANCEL_REASON        <> 'Upgraded'
    GROUP BY 1, 2
),
-- Cohort retention + first-month attrition: for each park x join month, the genuine new-member cohort and how many survive to each milestone.
-- Bucketed by join month. For each cohort, checks how many are still active at each month milestone.
-- Milestone = N dunning cycles of 33 days each, measured from each member's own join date (CANCEL_DAYS > 33*N). 33 days = the dunning/lapse window, so this is consistent with CHURN_FIRST_MONTH (CANCEL_DAYS <= 33) rather than calendar months.
mbr_retention AS (
    SELECT
          MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT SK_TICKET)                                                     AS COHORT_SIZE
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NOT NULL AND CANCEL_DAYS <= 33
                              THEN SK_TICKET END)                                        AS CHURN_FIRST_MONTH
        -- Retained at milestone N = survived past N months / dunning cycles (CANCEL_DAYS > 33*N) or never terminated.
        -- RETAINED_M1 = COHORT_SIZE - CHURN_FIRST_MONTH (CANCEL_DAYS > 33), so it stays the exact inverse of first-month attrition.
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 1  THEN SK_TICKET END) AS RETAINED_M1
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 2  THEN SK_TICKET END) AS RETAINED_M2
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 3  THEN SK_TICKET END) AS RETAINED_M3
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 4  THEN SK_TICKET END) AS RETAINED_M4
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 5  THEN SK_TICKET END) AS RETAINED_M5
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 6  THEN SK_TICKET END) AS RETAINED_M6
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 9  THEN SK_TICKET END) AS RETAINED_M9
        , COUNT(DISTINCT CASE WHEN SK_DATE_TERMINATION IS NULL OR CANCEL_DAYS > 33 * 12 THEN SK_TICKET END) AS RETAINED_M12_PLUS
    FROM (
        SELECT
              DATEADD('MONTH', 1, DATE_TRUNC('MONTH', SK_DATE_JOIN))  AS MONTH_START
            , SK_LOCATION_ACTIVE                                       AS SK_LOCATION
            , SK_TICKET
            , SK_DATE_TERMINATION
            , CANCEL_REASON
            , CANCEL_DAYS
        FROM mbr_facts_base
        WHERE SK_DATE_JOIN IS NOT NULL
          AND (CANCEL_REASON IS NULL OR CANCEL_REASON <> 'Upgraded')
    )
    GROUP BY 1, 2
),
-- Active member age buckets: distributes active members at end of month into tenure bands.
-- Uses day-level precision to avoid month-boundary artifacts.
mbr_active_age_buckets AS (
    SELECT
          pb.MONTH_START
        , pb.SK_LOCATION
        , COUNT(DISTINCT CASE WHEN DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) <   30 THEN f.SK_TICKET END) AS ACTIVE_AGE_LT1M
        , COUNT(DISTINCT CASE WHEN DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) >=  30
                               AND DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) <   91 THEN f.SK_TICKET END) AS ACTIVE_AGE_1TO3M
        , COUNT(DISTINCT CASE WHEN DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) >=  91
                               AND DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) <  183 THEN f.SK_TICKET END) AS ACTIVE_AGE_3TO6M
        , COUNT(DISTINCT CASE WHEN DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) >= 183
                               AND DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) <  274 THEN f.SK_TICKET END) AS ACTIVE_AGE_6TO9M
        , COUNT(DISTINCT CASE WHEN DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) >= 274
                               AND DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) <  365 THEN f.SK_TICKET END) AS ACTIVE_AGE_9TO12M
        , COUNT(DISTINCT CASE WHEN DATEDIFF('day', f.SK_DATE_JOIN, pb.MONTH_START) >= 365                       THEN f.SK_TICKET END) AS ACTIVE_AGE_12M_PLUS
    FROM parks_base pb
    JOIN mbr_facts_base f
        ON  f.SK_LOCATION_ACTIVE  = pb.SK_LOCATION
        AND f.SK_DATE_JOIN        < pb.MONTH_START
        AND (f.SK_DATE_TERMINATION IS NULL OR f.SK_DATE_TERMINATION >= pb.MONTH_START)
    GROUP BY 1, 2
)
SELECT
      DATEADD('DAY', -1, pb.MONTH_START)::DATE AS SK_DATE_RECORD  --> Last day of the reporting month; joins to GOLD_DB.DW.DIMDATE in AAS
    , pb.SK_LOCATION::INT AS SK_LOCATION                          --> To join with GOLD_DB.CNS.TBL_DIMLOCATION in AAS (always current SK)
    , am.ACTIVE_MEMBERS::FLOAT AS MEMBERS_ACTIVE
    , pb.POTENTIALS_TOTAL::FLOAT AS POTENTIALS_TOTAL
    , pb.POTENTIALS_INPARK::FLOAT AS POTENTIALS_INPARK
    , pb.POTENTIALS_ONLINE::FLOAT AS POTENTIALS_ONLINE
    , nm.NEW_MEMBERS_TOTAL::FLOAT AS NEW_MEMBERS_TOTAL
    , nm.NEW_MEMBERS_INPARK::FLOAT AS NEW_MEMBERS_INPARK
    , nm.NEW_MEMBERS_ONLINE::FLOAT AS NEW_MEMBERS_ONLINE
    , u.UPGRADES_TOTAL::FLOAT AS UPGRADES_TOTAL
    -- True new members: new members minus upgrade child tickets (child ticket joins in same month as upgrade)
    , (NVL(nm.NEW_MEMBERS_TOTAL,0) - NVL(u.UPGRADES_TOTAL,0))::FLOAT AS NEW_MEMBERS_EXCL_UPGRADES
    , pa.PARENT_ADDONS::FLOAT AS NEW_MEMBERS_PARENT_ADDONS
    , pa.PARENT_ADDONS_UNDER18::FLOAT AS PARENT_ADDONS_UNDER18
    , r.MEMBERS_REACTIVATED::FLOAT AS MEMBERS_REACTIVATED
    , c.CHURN_VOLUNTARY::FLOAT AS CHURN_VOLUNTARY
    , c.CHURN_INVOLUNTARY::FLOAT AS CHURN_INVOLUNTARY
    , c.CHURN_TOTAL::FLOAT AS CHURN_TOTAL
    -- NULL until 33 days after the end of the join cohort month (one month prior to the row label)
    , CASE WHEN CURRENT_DATE >= DATEADD(DAY, 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) 
            THEN rt.CHURN_FIRST_MONTH
            ELSE NULL
      END::FLOAT AS CHURN_FIRST_MONTH
    , (NVL(nm.NEW_MEMBERS_TOTAL,0)
        - NVL(u.UPGRADES_TOTAL,0)
        - NVL(c.CHURN_TOTAL,0))::FLOAT AS MEMBERS_NET_CHANGE
    , o.OSAT_MEMBER_COUNT::FLOAT AS OSAT_MEMBER_COUNT
    , o.OSAT_5::FLOAT AS OSAT_5
    , o.OSAT_4::FLOAT AS OSAT_4
    , o.OSAT_3::FLOAT AS OSAT_3
    , o.OSAT_2::FLOAT AS OSAT_2
    , o.OSAT_1::FLOAT AS OSAT_1
    , sk.SOCKS_W_MBR_CAPPED::FLOAT AS SOCKS_W_MBR_CAPPED
    , sk.SOCKS_W_MBR_CAPPED_INPARK::FLOAT AS SOCKS_W_MBR_CAPPED_INPARK
    , (sk.SOCKS_W_MBR_CAPPED_INPARK / NULLIF(nm.NEW_MEMBERS_INPARK, 0))::FLOAT AS SOCK_ATTACH_RATE_INPARK
    , rc.RECURRING_DUES_COLLECTED::FLOAT AS RECURRING_DUES_COLLECTED
    , nc.NEW_DUES_COLLECTED_WITH_REFUNDS::FLOAT AS NEW_DUES_COLLECTED_WITH_REFUNDS
    , ad.DUR_CHURN_AVG_MONTHS::FLOAT AS DUR_CHURN_AVG_MONTHS
    -- Weighted avg components for region/district rollup only — do NOT use as standalone metrics.
    , ad.DUR_CHURN_WTAVG_DAYS_SUM::FLOAT AS DUR_CHURN_WTAVG_DAYS_SUM
    , ad.DUR_CHURN_WTAVG_MEMBER_COUNT::FLOAT AS DUR_CHURN_WTAVG_MEMBER_COUNT
    , d12.DUR_CHURN_L12M_AVG_MONTHS::FLOAT AS DUR_CHURN_L12M_AVG_MONTHS
    -- Weighted avg components for region/district rollup only — do NOT use as standalone metrics.
    , d12.DUR_CHURN_L12M_WTAVG_DAYS_SUM::FLOAT AS DUR_CHURN_L12M_WTAVG_DAYS_SUM
    , d12.DUR_CHURN_L12M_WTAVG_MEMBER_COUNT::FLOAT AS DUR_CHURN_L12M_WTAVG_MEMBER_COUNT
    , rt.COHORT_SIZE::FLOAT AS RETENTION_COHORT_SIZE
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M1       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M1
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M2       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M2
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M3       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M3
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M4       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M4
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M5       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M5
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M6       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M6
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M9       ELSE NULL END::FLOAT AS RETENTION_RETAINED_M9
    , CASE WHEN CURRENT_DATE >= DATEADD('DAY', 33, LAST_DAY(DATEADD('MONTH', -1, pb.MONTH_START))) THEN rt.RETAINED_M12_PLUS ELSE NULL END::FLOAT AS RETENTION_RETAINED_M12_PLUS
    -- Active member age distribution at end of month
    , ab.ACTIVE_AGE_LT1M::FLOAT AS ACTIVE_AGE_LT1M
    , ab.ACTIVE_AGE_1TO3M::FLOAT AS ACTIVE_AGE_1TO3M
    , ab.ACTIVE_AGE_3TO6M::FLOAT AS ACTIVE_AGE_3TO6M
    , ab.ACTIVE_AGE_6TO9M::FLOAT AS ACTIVE_AGE_6TO9M
    , ab.ACTIVE_AGE_9TO12M::FLOAT AS ACTIVE_AGE_9TO12M
    , ab.ACTIVE_AGE_12M_PLUS::FLOAT AS ACTIVE_AGE_12M_PLUS
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
LEFT JOIN mbr_churned_by_reason c
    ON  c.SK_LOCATION = pb.SK_LOCATION
    AND c.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_parent_addon pa
    ON  pa.SK_LOCATION = pb.SK_LOCATION
    AND pa.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_osat o
    ON  o.SK_LOCATION = pb.SK_LOCATION
    AND o.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_socks sk
    ON  sk.SK_LOCATION = pb.SK_LOCATION
    AND sk.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_recurring_collected rc
    ON  rc.SK_LOCATION = pb.SK_LOCATION
    AND rc.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_new_collected nc
    ON  nc.SK_LOCATION = pb.SK_LOCATION
    AND nc.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_churned_avg_duration ad
    ON  ad.SK_LOCATION = pb.SK_LOCATION
    AND ad.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_churned_avg_duration_l12m d12
    ON  d12.SK_LOCATION = pb.SK_LOCATION
    AND d12.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_retention rt
    ON  rt.SK_LOCATION = pb.SK_LOCATION
    AND rt.MONTH_START = pb.MONTH_START
LEFT JOIN mbr_active_age_buckets ab
    ON  ab.SK_LOCATION = pb.SK_LOCATION
    AND ab.MONTH_START = pb.MONTH_START
-- Only show months where the full calendar month is complete
WHERE pb.MONTH_START <= DATE_TRUNC('MONTH', CURRENT_DATE)
ORDER BY pb.MONTH_START DESC
;