WITH cte_visits AS
(
select    ds as TIM_DAY
        , application.site_id as SIT_SITE_ID
        , SUBSTR(CAST(JSON_EXTRACT_SCALAR(tracks.event_data, "$['item_id']")    AS STRING),4,20)   AS ITE_ITEM_ID
        , CASE WHEN device.platform like '/web/desktop%' THEN '/web/desktop'
               WHEN device.platform like '/web/mobile%'  THEN '/web/mobile'
               ELSE  device.platform
           END AS DEVICE
        , JSON_EXTRACT_SCALAR(platform.http.headers,'$.x-platform') as PLATFORM  
        , trim(application.business)AS BU
        ,path
        , count(distinct usr.uid) as qty_visits,
          count(distinct (case when  path = '/pdp' then usr.uid end)) as qty_visits_catalog,
          count(distinct (case when  path = '/vip' then usr.uid end)) as qty_visits_vip,
          count(1) as qty_pageviews,
          count (case when path = '/pdp' then 1 end) as  qty_pageviews_catalog
        , count (case when path = '/vip' then 1 end) as  qty_pageviews_vip
FROM `meli-bi-data.MELIDATA.TRACKS` AS tracks
where ds = current_date -1 --between current_date -6 and current_date-4
AND( path = '/vip' or path = '/pdp') --or path = 'mlvipviewcontroller' 
and application.business in ('mercadolibre','portalinmobiliario')
AND CAST(JSON_EXTRACT_SCALAR(tracks.event_data, "$['seller_id']") AS STRING) IS NOT NULL
AND ( (device.platform like '/mobile%' and platform.mobile.mode = 'deferred' or platform.mobile.mode = 'normal') 
or (device.platform like '/web%') )
   AND NOT (      device.user_agent  LIKE '%libwww%'
            OR device.user_agent  LIKE '%wget%'
            OR device.user_agent  LIKE '%lwp%'
            OR device.user_agent  LIKE '%damnBot%'
            OR device.user_agent  LIKE '%bbbike%'
            OR device.user_agent  LIKE '%java%'
            OR device.user_agent  LIKE '%spider%'
            OR device.user_agent  LIKE '%crawl%'
            OR device.user_agent  LIKE '%slurp%'
            OR device.user_agent  LIKE '%bot%'
            OR device.user_agent  LIKE '%feedburner%'
            OR device.user_agent  LIKE '%googleimageproxy%'
            OR device.user_agent  LIKE '%google web preview%'
            OR device.user_agent  LIKE '%whatsapp%'
            OR device.user_agent  LIKE '%riddler%'
            OR device.user_agent  LIKE '%scrapy%'
            OR device.user_agent  LIKE '%facebookexternalhit%'
            OR device.user_agent  LIKE '%cubot%'
            OR device.user_agent  LIKE '%mediapartners-google%'
            OR device.user_agent  LIKE '%apis-google%'
            OR device.user_agent  LIKE '%feedfetcher-google%'
            OR device.user_agent  LIKE '%duplexweb-google%'
            OR device.user_agent  LIKE '%google favicon%'
            OR device.user_agent  LIKE '%googleweblight%' )
group by    ds 
          , application.site_id, 
          CASE WHEN device.platform like '/web/desktop%' THEN '/web/desktop'
               WHEN device.platform like '/web/mobile%'  THEN '/web/mobile'
               ELSE  device.platform
           END 
          , JSON_EXTRACT_SCALAR(platform.http.headers,'$.x-platform')  
          , trim(application.business) 
         , SUBSTR(CAST(JSON_EXTRACT_SCALAR(tracks.event_data, "$['item_id']")    AS STRING),4,20)
         , path
)   
SELECT 
          TIM_DAY 
        , SUBSTR(regexp_replace(regexp_replace(SIT_SITE_ID ,'\r',''),'\n',''),0,20) as SIT_SITE_ID 
        , SUBSTR(regexp_replace(regexp_replace(ITE_ITEM_ID,'\r',''),'\n',''),0,60) as ITE_ITEM_ID 
        , SUBSTR(regexp_replace(regexp_replace(platform,'\r',''),'\n',''),0,100) as PLATFORM
        , SUBSTR(regexp_replace(regexp_replace(DEVICE,'\r',''),'\n',''),0,50) as DEVICE
        , SUBSTR(regexp_replace(regexp_replace(BU,'\r',''),'\n',''),0,100) as BU
        , SUM(QTY_VISITS)            AS QTY_VISITS
        , SUM(QTY_VISITS_VIP)        AS QTY_VISITS_VIP
        , SUM(QTY_VISITS_CATALOG)    AS QTY_VISITS_CATALOG
        , SUM(QTY_PAGEVIEWS)         AS QTY_PAGEVIEWS
        , SUM(QTY_PAGEVIEWS_VIP)     AS QTY_PAGEVIEWS_VIP
        , SUM(QTY_PAGEVIEWS_CATALOG) AS QTY_PAGEVIEWS_CATALOG
        , TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -4 HOUR)         AUD_INS_DTTM
        , TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -4 HOUR)         AUD_UPD_DTTM
        , DATE(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -4 HOUR))   AUD_UPD_DT
        , CAST(0 AS INT64)                                             AUD_TRANSACTION_ID
        , 'SCHEDULE_BIGQUERY_VISITS_RT'                                AUD_FROM_INTERFACE 
from cte_visits
WHERE SIT_SITE_ID != 'UNKNOWN'
group by TIM_DAY , 
         SIT_SITE_ID , 
         ite_item_id,
         platform,
         DEVICE, 
         BU