SELECT
td_id,
last_interaction_days
FROM
  `abi-martech-global.maz_col_cdp_inbound.L2_attributes`
WHERE
  classification_category != 'bronze'
  AND td_id IN (
  SELECT
    td_id
  FROM
    `abi-martech-global.maz_col_cdp_inbound.L2_brand_behaviors`
  WHERE
    LOWER(abi_brand) LIKE '%guila%'
    OR LOWER(abi_brand) LIKE '%poker%'
    OR LOWER(abi_brand) LIKE '%club%'
    OR LOWER(abi_brand) LIKE '%corona%' )
 AND td_id NOT IN ('00013c98-e4fd-4532-8b4f-c3eeb9d93aff', '00001e34-68b2-4dfd-802b-cccdcdd1bc3c')