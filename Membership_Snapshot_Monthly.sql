WITH
-- Base: park x month from revenue, sum pre-calculated potentials split by booking channel
parks_base AS (
    SELECT
          DATE_TRUNC('MONTH', fr.SK_DATE_RECORD)                                                    AS MONTH_START
        , fr.SK_LOCATION
        --Bucketing all potentials not sold online to 'In Store' for consistency
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED <> 'Online Sales'  THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_INPARK
        , SUM(CASE WHEN db.BOOKINGLOCATIONSTANDARDIZED = 'Online Sales' THEN fr.POTENTIALS ELSE 0 END)  AS POTENTIALS_ONLINE
        , SUM(fr.POTENTIALS)                                                                        AS POTENTIALS_TOTAL
    FROM PROD_EDW2_SHARE_GOLD_DB.CNS.TBL_FACTREVENUE fr
    LEFT JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMBOOKING db
        ON db.SK_BOOKING = fr.SK_BOOKING
    GROUP BY 1, 2
),

-- Reactivated members per park per month
-- A reactivation is any event where SK_EVENTTYPE = 6 ('Reactivated') in FACTMEMBERSHIPPASSEVENTS
mbr_reactivated AS (
    SELECT
          DATE_TRUNC('MONTH', fm.SK_DATE)          AS MONTH_START
        , fm.SK_LOCATION
        , COUNT(DISTINCT fm.SK_TICKET)             AS REACTIVATED
    FROM PROD_EDW2_SHARE_GOLD_DB.DW.FACTMEMBERSHIPPASSEVENTS fm
    JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMMEMBERSHIPPASSEVENT dm
        USING (SK_EVENTTYPE)
    WHERE dm.EVENTTYPE = 'Reactivated'
    GROUP BY 1, 2
),

-- Active member count at end of each month per park
-- A member is active if they joined on or before month-end and their termination date has not yet passed — regardless of payment or cancellation status
mbr_active AS (
    SELECT
          pb.MONTH_START
        , f.SK_LOCATION
        , COUNT(DISTINCT f.sk_ticket)       AS ACTIVE_MEMBERS
    FROM parks_base pb
    JOIN SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS f
        ON  f.SK_LOCATION         = pb.SK_LOCATION

        AND f.JOIN_DATE         < pb.MONTH_START
        AND (f.TERMINATION_DATE IS NULL OR f.TERMINATION_DATE >= pb.MONTH_START)
    GROUP BY 1, 2
),

-- New members added per park per month (by join date), split by booking channel
-- Includes all tickets with a join date — genuine new sales AND upgrade child tickets
-- CONV_TYPE values: 'In Store', 'Online Sales', 'Data Import', 'Venue Manager', NULL
mbr_new AS (
    SELECT
          DATE_TRUNC('MONTH', JOIN_DATE)                                              AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT CASE WHEN CONV_TYPE <> 'Online Sales'THEN TICKETID END)     AS NEW_MEMBERS_INPARK
        , COUNT(DISTINCT CASE WHEN CONV_TYPE = 'Online Sales' THEN TICKETID END)     AS NEW_MEMBERS_ONLINE
        , COUNT(DISTINCT TICKETID)                                                    AS NEW_MEMBERS_TOTAL
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE JOIN_DATE IS NOT NULL
    GROUP BY 1, 2
),

-- Upgrades per park per month (by upgrade date), split by prior status
mbr_upgrades AS (
    SELECT
          DATE_TRUNC('MONTH', UPGRADE_DATE)                                                               AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT TICKETID)                                                             AS UPGRADES_TOTAL
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE ATTRITION_REASON = 'Upgraded'
    GROUP BY 1, 2
),

-- Cancellations broken out by reason, per park per month (using SK_ATTRITION_DATE)
mbr_churn AS (
    SELECT
          DATE_TRUNC('MONTH', ATTRITION_DATE)                                                   AS MONTH_START
        , SK_LOCATION
        -- Voluntary: member-initiated or non-renewal, bucketing all other cancels so the totals match
        -- BUG FIX: Voluntary includes ('Cancel Requested', 'Refund', 'Cancel Assumed') and the bug 'Term Roller'
        , SUM(CASE WHEN ATTRITION_REASON NOT IN ('Payment Issue', 'Lapsed') AND ATTRITION_REASON IS NOT NULL THEN 1 ELSE 0 END) AS CHURN_VOLUNTARY
        -- Involuntary: billing failure or lapse due to non-payment
        , SUM(CASE WHEN ATTRITION_REASON IN ('Payment Issue', 'Lapsed')                                     THEN 1 ELSE 0 END) AS CHURN_INVOLUNTARY
        , COUNT(*)                                                                                           AS CANCELS_TOTAL
        -- cancelled before day 33 (# of days before a failed recharge is turned to a missed payment), excludes upgrades
        , SUM(CASE WHEN ATTRITION_DAYS <= 33
                        AND ATTRITION_REASON <> 'Upgraded' THEN 1 ELSE 0 END) AS CHURN_FIRST_MONTH
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE ATTRITION_DATE IS NOT NULL
    GROUP BY 1, 2
),

-- Parent add-on memberships sold per park per month
-- Identified by PRODUCT_NAME containing 'parent' (case-insensitive)
mbr_parent_addon AS (
    SELECT
          DATE_TRUNC('MONTH', JOIN_DATE)       AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT TICKETID)             AS PARENT_ADDONS
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_EVENTS
    WHERE JOIN_DATE IS NOT NULL
      AND PRODUCT_NAME ILIKE '%parent%'
    GROUP BY 1, 2
)

SELECT
      pb.MONTH_START
    , dl.LOCATIONID
    , dl.BUSINESSGROUP                         AS BUSINESS_GROUP
    , COALESCE(am.ACTIVE_MEMBERS,          0)  AS ACTIVE_MEMBERS
    , COALESCE(pb.POTENTIALS_TOTAL,        0)  AS POTENTIALS_TOTAL
    , COALESCE(nm.NEW_MEMBERS_TOTAL,       0)  AS NEW_MEMBERS_TOTAL
    , COALESCE(u.UPGRADES_TOTAL,           0)  AS UPGRADES_TOTAL
    -- True new sales: new members minus upgrade child tickets (child ticket joins in same month as upgrade)
    , COALESCE(nm.NEW_MEMBERS_TOTAL, 0)
        - COALESCE(u.UPGRADES_TOTAL, 0)        AS NEW_MEMBERS_EXCL_UPGRADES
    , COALESCE(c.CHURN_VOLUNTARY,          0)  AS CHURN_VOLUNTARY
    , COALESCE(c.CHURN_INVOLUNTARY,        0)  AS CHURN_INVOLUNTARY
    , COALESCE(c.CANCELS_TOTAL,            0)  AS CHURN_TOTAL
    , COALESCE(nm.NEW_MEMBERS_TOTAL, 0)
        - COALESCE(u.UPGRADES_TOTAL, 0)
        - COALESCE(c.CANCELS_TOTAL, 0)         AS NET_MEMBER_CHANGE
    -- NULL until 33 days after month-end (33 days = window before a failed recharge becomes a missed payment)
    , CASE WHEN CURRENT_DATE >= DATEADD(DAY, 33, LAST_DAY(pb.MONTH_START))
           THEN COALESCE(c.CHURN_FIRST_MONTH, 0)
           ELSE NULL
      END                                      AS CHURN_FIRST_MONTH
    , COALESCE(r.REACTIVATED,              0)  AS REACTIVATED
    , COALESCE(pb.POTENTIALS_ONLINE,       0)  AS POTENTIALS_ONLINE
    , COALESCE(pb.POTENTIALS_INPARK,       0)  AS POTENTIALS_INPARK
    , COALESCE(nm.NEW_MEMBERS_INPARK,      0)  AS NEW_MEMBERS_INPARK
    , COALESCE(nm.NEW_MEMBERS_ONLINE,      0)  AS NEW_MEMBERS_ONLINE
    , COALESCE(pa.PARENT_ADDONS,          0)  AS PARENT_ADDONS
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
WHERE dl.LOCATIONID = 'Aurora, CO - 130'
ORDER BY pb.MONTH_START DESC, dl.LOCATIONID
;
