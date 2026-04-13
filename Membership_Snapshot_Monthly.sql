-- Membership Overview by Park by Month
-- Sources:
--   PROD_EDW2_SHARE_GOLD_DB.DW.FACTREVENUE              -> base (park x month grain), pre-calculated POTENTIALS
--   SKYZONE_BI.DATAMART.MEMBERSHIP_LAST_EVENT -> active members, new members, cancel reasons

WITH

-- Base: park x month from revenue, sum pre-calculated potentials
parks_base AS (
    SELECT
          DATE_TRUNC('MONTH', SK_DATE_RECORD) AS MONTH_START
        , SK_LOCATION
        , SUM(POTENTIALS)                     AS POTENTIALS
    FROM PROD_EDW2_SHARE_GOLD_DB.DW.FACTREVENUE
    GROUP BY 1, 2
),

-- Active member count at end of each month per park
-- Filtered to currently active members (ACTIVE_STATUS = 1)
-- who joined on or before month-end and have not been terminated by month-end
active_members AS (
    SELECT
          pb.MONTH_START
        , f.SK_LOCATION
        , COUNT(DISTINCT f.TICKETID) AS ACTIVE_MEMBERS
    FROM parks_base pb
    JOIN SKYZONE_BI.DATAMART.MEMBERSHIP_LAST_EVENT f
        ON  f.SK_LOCATION     = pb.SK_LOCATION
        AND f.SK_JOIN_DATE    <= LAST_DAY(pb.MONTH_START)
        AND (f.SK_TERMINATION_DATE IS NULL OR f.SK_TERMINATION_DATE > LAST_DAY(pb.MONTH_START))
    WHERE f.ACTIVE_STATUS = 1
    GROUP BY 1, 2
),

-- New members added per park per month (by join date)
-- NEW_MEMBERS includes all tickets (including upgrade child tickets)
-- NEW_MEMBERS_EXCL_UPGRADES excludes tickets whose current status is an upgrade
new_members AS (
    SELECT
          DATE_TRUNC('MONTH', SK_JOIN_DATE)                                                                          AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT TICKETID)                                                                                   AS NEW_MEMBERS
        , COUNT(DISTINCT CASE WHEN MEMBERSHIP_CURRENT_STATUS NOT IN ('Upgraded From Active', 'Upgraded From Inactive')
                              THEN TICKETID END)                                                                     AS NEW_MEMBERS_EXCL_UPGRADES
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_LAST_EVENT
    WHERE SK_JOIN_DATE IS NOT NULL
    GROUP BY 1, 2
),

-- Upgrades per park per month (by upgrade date), split by prior status
upgrades AS (
    SELECT
          DATE_TRUNC('MONTH', SK_UPGRADE_DATE)                                                               AS MONTH_START
        , SK_LOCATION
        , COUNT(DISTINCT CASE WHEN MEMBERSHIP_CURRENT_STATUS = 'Upgraded From Active'   THEN TICKETID END)  AS UPGRADED_FROM_ACTIVE
        , COUNT(DISTINCT CASE WHEN MEMBERSHIP_CURRENT_STATUS = 'Upgraded From Inactive' THEN TICKETID END)  AS UPGRADED_FROM_INACTIVE
        , COUNT(DISTINCT TICKETID)                                                                           AS UPGRADES_TOTAL
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_LAST_EVENT
    WHERE MEMBERSHIP_CURRENT_STATUS IN ('Upgraded From Active', 'Upgraded From Inactive')
    GROUP BY 1, 2
),

-- Cancellations broken out by reason, per park per month (using SK_ATTRITION_DATE)
cancels AS (
    SELECT
          DATE_TRUNC('MONTH', SK_ATTRITION_DATE)                                                   AS MONTH_START
        , SK_LOCATION
        , SUM(CASE WHEN ATTRITION_REASON IN ('Assumed Cancelled', 'Cancelled', 'Cancel Requested')  THEN 1 ELSE 0 END) AS CANCELLED
        , SUM(CASE WHEN ATTRITION_REASON = 'Payment Issue'                                          THEN 1 ELSE 0 END) AS PAYMENT_ISSUE
        , SUM(CASE WHEN ATTRITION_REASON = 'Refund'                                                 THEN 1 ELSE 0 END) AS REFUND
        , COUNT(*)                                                                                  AS CANCELS_TOTAL
        -- cancelled before day 33 (# of days before a failed recharge is turned to a missed payment), excludes upgrades
        , SUM(CASE WHEN ATTRITION_DAYS < 33
                        AND MEMBERSHIP_CURRENT_STATUS NOT IN ('Upgraded From Active', 'Upgraded From Inactive') THEN 1 ELSE 0 END) AS CANCELS_FIRST_MONTH  
        -- cancelled before first recurring payment, excludes upgrades
        , SUM(CASE WHEN (RECURR_PAY_COUNT = 0 OR RECURR_PAY_COUNT IS NULL)
                        AND MEMBERSHIP_CURRENT_STATUS NOT IN ('Upgraded From Active', 'Upgraded From Inactive') THEN 1 ELSE 0 END) AS CANCELS_PRE_RECURRING  
    FROM SKYZONE_BI.DATAMART.MEMBERSHIP_LAST_EVENT
    WHERE SK_ATTRITION_DATE IS NOT NULL
    GROUP BY 1, 2
)

SELECT
      pb.MONTH_START
    , dl.LOCATIONID
    , dl.BUSINESSGROUP                         AS BUSINESS_GROUP
    , COALESCE(am.ACTIVE_MEMBERS,          0)  AS ACTIVE_MEMBERS
    , COALESCE(pb.POTENTIALS,              0)  AS POTENTIALS
    , COALESCE(nm.NEW_MEMBERS,                  0)  AS NEW_MEMBERS
    , COALESCE(nm.NEW_MEMBERS_EXCL_UPGRADES,    0)  AS NEW_MEMBERS_EXCL_UPGRADES
    , COALESCE(u.UPGRADED_FROM_ACTIVE,     0)  AS UPGRADED_FROM_ACTIVE
    , COALESCE(u.UPGRADED_FROM_INACTIVE,   0)  AS UPGRADED_FROM_INACTIVE
    , COALESCE(u.UPGRADES_TOTAL,           0)  AS UPGRADES_TOTAL
    , COALESCE(c.CANCELLED,                0)  AS CANCELLED
    , COALESCE(c.PAYMENT_ISSUE,            0)  AS PAYMENT_ISSUE
    , COALESCE(c.REFUND,                   0)  AS REFUNDED
    , COALESCE(c.CANCELS_TOTAL,            0)  AS CANCELS_TOTAL
    , COALESCE(nm.NEW_MEMBERS_EXCL_UPGRADES, 0)
        - COALESCE(c.CANCELS_TOTAL, 0)         AS NET_MEMBER_CHANGE
    -- NULL until 33 days after month-end (33 days = window before a failed recharge becomes a missed payment)
    , CASE WHEN CURRENT_DATE >= DATEADD(DAY, 33, LAST_DAY(pb.MONTH_START))
           THEN COALESCE(c.CANCELS_FIRST_MONTH,  0)
           ELSE NULL
      END                                      AS CANCELS_FIRST_MONTH    
    , CASE WHEN CURRENT_DATE >= DATEADD(DAY, 33, LAST_DAY(pb.MONTH_START))
           THEN COALESCE(c.CANCELS_PRE_RECURRING, 0)
           ELSE NULL
      END                                      AS CANCELS_PRE_RECURRING  
FROM parks_base pb
LEFT JOIN PROD_EDW2_SHARE_GOLD_DB.DW.DIMLOCATION dl
    ON  dl.SK_LOCATION     = pb.SK_LOCATION
LEFT JOIN active_members am
    ON  am.SK_LOCATION = pb.SK_LOCATION
    AND am.MONTH_START = pb.MONTH_START
LEFT JOIN new_members nm
    ON  nm.SK_LOCATION = pb.SK_LOCATION
    AND nm.MONTH_START = pb.MONTH_START
LEFT JOIN upgrades u
    ON  u.SK_LOCATION = pb.SK_LOCATION
    AND u.MONTH_START = pb.MONTH_START
LEFT JOIN cancels c
    ON  c.SK_LOCATION = pb.SK_LOCATION
    AND c.MONTH_START = pb.MONTH_START
ORDER BY pb.MONTH_START DESC, dl.LOCATIONID
;
