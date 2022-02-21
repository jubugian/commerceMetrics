WITH 
dateFrom AS (SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) 

, ga_sessions AS (

      SELECT date
            , site
            , (SELECT value FROM hits.customDimensions WHERE index=49 ) AS itemID
            , CONCAT(fullVisitorId,SAFE_CAST(visitStartTime AS STRING)) AS SessionId
            , fullVisitorId as fullVisitorId
            , CASE
                WHEN hits.appInfo.appName IS NOT NULL THEN (totals.hits = 1 AND COALESCE(hits.isExit,FALSE))
                ELSE NOT (totals.bounces is null)
                END  AS  isBounce
            , totals.newVisits AS newVisits
      FROM `calm-mariner-105612.ALL_GA360.ga_sessions` , unnest(hits) as hits
      WHERE date = (SELECT * FROM dateFrom)
      AND businessUnit = 'ML'
      AND site IN ('MLC','MLU','MPE','MLA','MLM','MLB','MLV','MCO')

)
SELECT DATE
     , Site
     , itemID
     , COUNT(DISTINCT SessionId) AS sessions
     , COUNT(DISTINCT IF(NOT isBounce, SessionId, NULL)) AS sessionsNotBounce
     , COUNT(DISTINCT fullVisitorId ) AS Users
     , COUNT(DISTINCT IF(newVisits=1, fullVisitorId, NULL)) AS newUsers
FROM ga_sessions
GROUP BY 1,2,3
