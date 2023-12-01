WITH 
    t1 AS(
    SELECT
      td_id AS td_id1
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_all_attributes`
    WHERE
      classification_category IN ('gold',
        'diamond')
    AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189 ),

t4 AS (
    SELECT
      td_id,
      COUNT(*) AS numero_faceads,
      MAX(time) AS maxfb
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_facebook_ads_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY
      1),
    t5 AS (
    SELECT
      td_id,
      COUNT(*) AS numero_page_clicks,
      MAX(time) AS max_page_clicks
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_page_clicks_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY
      1),
        t55 AS (
    SELECT
      td_id,
      COUNT(*) AS numero_uploads,
      MAX(time) AS maxup
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_file_uploads_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY
      1),  
    t10 AS (
    SELECT
      td_id,
      COUNT(*) AS numero_sfmc,
      MAX(time) AS maxsfmc
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_sfmc_events_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
      AND abi_event_type IN ('open',
        'click')
    GROUP BY
      1),
      t9 AS (
    SELECT
      td_id,
      COUNT(*) AS numero_web,
      MAX(time) AS maxweb
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_web_form_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY
      1),
      t30 as (
      SELECT
      td_id,
      COUNT(*) AS numero_ecom,
      MAX(time) AS max_ecom
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_ecommerce_customers_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY 1
      ),
      t31 as (
      select
      td_id,
      COUNT(*) AS numero_loyal,
      MAX(time) AS maxloyal
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_loyalty_customers_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY
      1),
      t32 as (
      select
      td_id,
      COUNT(*) AS numero_views,
      MAX(time) AS maxviews
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_page_views_with_td_id`
    WHERE
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
    GROUP BY
      1),
      brand_1 as (
        select
        distinct td_id,
        abi_brand
        FROM `abi-martech-global.maz_col_cdp_inbound.L2_brand_behaviors`
        where lower(abi_brand) like '%guila%'
      OR LOWER(abi_brand) LIKE '%poker%'
      OR LOWER(abi_brand) LIKE '%club%'
      OR LOWER(abi_brand) LIKE '%corona%' 
      AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(time), DAY) <= 189
      group by 1,2
      ),
tt11 as (
      SELECT
      t1.td_id1,
      IFNULL(t4.numero_faceads,0) AS numero_faceads,
      IFNULL(t5.numero_page_clicks,0) AS numero_page_clicks,
      IFNULL(t55.numero_uploads,0) AS numero_uploads,
      IFNULL(t10.numero_sfmc,0) AS numero_sfmc,
      IFNULL(t9.numero_web,0) AS numero_web,
      IFNULL(t30.numero_ecom,0) AS numero_ecom,
      IFNULL(t31.numero_loyal,0) AS numero_loyal,
      IFNULL(t32.numero_views,0) AS numero_views,
      IFNULL(t4.numero_faceads,0)+
      IFNULL(t5.numero_page_clicks,0)+
      IFNULL(t55.numero_uploads,0)+
      IFNULL(t30.numero_ecom,0)+
      IFNULL(t10.numero_sfmc,0)+
      IFNULL(t31.numero_loyal,0)+
      IFNULL(t32.numero_views,0) AS nInteracciones,
      TIMESTAMP_SECONDS(GREATEST(IFNULL(t4.maxfb,0),
      IFNULL(t5.max_page_clicks,0), 
      IFNULL(t30.max_ecom,0), IFNULL(t32.maxviews,0), IFNULL(t55.maxup,0),
      IFNULL(t10.maxsfmc,0), IFNULL(t9.maxweb,0), IFNULL(t31.maxloyal,0))) AS Tmax_interacciones,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(GREATEST(IFNULL(t4.maxfb,0),
      IFNULL(t5.max_page_clicks,0),
      IFNULL(t30.max_ecom,0),
      IFNULL(t31.maxloyal,0),
      IFNULL(t32.maxviews,0),
      IFNULL(t55.maxup,0),
      IFNULL(t10.maxsfmc,0), IFNULL(t9.maxweb,0))), DAY) AS Dias_interaccion,
      FROM
   t1
    LEFT JOIN
      t4
    ON
      t1.td_id1 = t4.td_id
    LEFT JOIN
      t5
    ON
      t1.td_id1 = t5.td_id
    LEFT JOIN
      t9
    ON
      t1.td_id1 = t9.td_id
    LEFT JOIN
      t10
    ON
      t1.td_id1 = t10.td_id
    LEFT JOIN
      t30
    ON
      t1.td_id1 = t30.td_id  
    LEFT JOIN
      t31
    ON
      t1.td_id1 = t31.td_id  
    LEFT JOIN
      t32
    ON
      t1.td_id1 = t32.td_id   
    LEFT JOIN
      t55
    ON
      t1.td_id1 = t55.td_id 
        
      )

      select
      tt11.td_id1,
      nInteracciones,
      Dias_interaccion,
      -- abi_brand
      from
      tt11
      LEFT JOIN 
        brand_1
        on tt11.td_id1 = brand_1.td_id
      where td_id1 != '00013c98-e4fd-4532-8b4f-c3eeb9d93aff'
      and td_id1 != '00001e34-68b2-4dfd-802b-cccdcdd1bc3c'
      and Tmax_interacciones >= '2023-01-01'
      and abi_brand is not null
      and nInteracciones > 0
      group by 1,2,3
      order by Dias_interaccion desc
    


