WITH 
    t1 AS(
    SELECT
      td_id AS td_id1
    FROM
      `abi-martech-global.maz_col_cdp_inbound.l1_col_all_attributes`
    WHERE
      classification_category IN ('gold',
        'diamond')),
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
      brand_1 as (
        select
        td_id,
        abi_brand
        FROM `abi-martech-global.maz_col_cdp_inbound.L2_brand_behaviors`
        where lower(abi_brand) like '%pok%'
      ),
tt11 as (
      SELECT
      t1.td_id1,
      IFNULL(t4.numero_faceads,0) AS numero_faceads,
      IFNULL(t5.numero_uploads,0) AS numero_uploads,
      IFNULL(t10.numero_sfmc,0) AS numero_sfmc,
      IFNULL(t9.numero_web,0) AS numero_web,
      IFNULL(t4.numero_faceads,0)+IFNULL(t5.numero_uploads,0)+
      IFNULL(t10.numero_sfmc,0) AS nInteracciones,
      TIMESTAMP_SECONDS(GREATEST(IFNULL(t4.maxfb,0),IFNULL(t5.maxup,0),
      IFNULL(t10.maxsfmc,0), IFNULL(t9.maxweb,0))) AS Tmax_interacciones,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),TIMESTAMP_SECONDS(GREATEST(IFNULL(t4.maxfb,0),IFNULL(t5.maxup,0),
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
      t1.td_id1 = t10.td_id)

      select
      tt11.td_id1,
      nInteracciones,
      Dias_interaccion,
      abi_brand
      from
      tt11
      LEFT JOIN 
        brand_1
        on tt11.td_id1 = brand_1.td_id
      where td_id1 != '00013c98-e4fd-4532-8b4f-c3eeb9d93aff'
      and td_id != '00001e34-68b2-4dfd-802b-cccdcdd1bc3c'
      and Tmax_interacciones >= '2023-01-01'
      and abi_brand is not null
      group by 1,2,3,4
      order by Dias_interaccion desc
    


