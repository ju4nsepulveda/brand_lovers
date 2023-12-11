WITH 
orders as
(
SELECT
ITEM_SKU,
ITEM_NAME,
ORDER_NK,
ORDER_DATETIME,
CUSTOMER_NK,
UNIT_NET_REVENUE_LOCAL,
DELIVERY_ADDRESS_STATE,
DATE_TRUNC(ORDER_DATE, MONTH) AS MES
FROM
`abi-martech-maz-col.maz_cds_columbus.VW_FACT_ORDER_DETAILS`
WHERE ORDER_STATUS_TYPE = 'Valid'
AND DATE_DIFF(CURRENT_DATE(), ORDER_DATE, DAY) <= 189
)
,
revenue as (
SELECT
  ORDER_NK,
  DATE_TRUNC(ORDER_DATETIME, MONTH) AS mes,
  SUM(UNIT_NET_REVENUE_LOCAL) AS plata
FROM
  orders
GROUP BY
  1, 2
),
str as (
  select
  VTEX_ID, MARCA
  from
  `abi-martech-maz-col.maz_cds_columbus.STR_BASE_PRODUCTO`
  where MARCA in ('Aguila', 'Club Colombia', 'Corona', 'Poker')

),
master as (
  select
  td_id,
  customer_nk
  from
  `abi-martech-maz-col.tadabra.Master_Tada_acumulado`
  where td_id != '00013c98-e4fd-4532-8b4f-c3eeb9d93aff'
  and td_id != '00001e34-68b2-4dfd-802b-cccdcdd1bc3c'
),

base as (

select
td_id,
orders.CUSTOMER_NK,
ITEM_SKU,
ITEM_NAME,
ORDER_NK,
ORDER_DATETIME,
UNIT_NET_REVENUE_LOCAL as money,
DELIVERY_ADDRESS_STATE, 
MES
from
orders
inner join(
  select
  *
  from
  master
) a
on orders.customer_nk = a.customer_nk
where orders.customer_nk not in (
  select
  distinct CUSTOMER_NK from `abi-martech-maz-col.tadabra.Fraud_customers`
)
)
,
conteo as (
select
count(distinct ORDER_NK) ordenes,
td_id
from
base
group by 2
order by 1 desc),

meses as (
select
count(distinct MES) conteo_mes,
td_id
from
base
group by 2
order by 1 desc),

conteo_total as (
  select
  conteo.td_id,
  ordenes,
  conteo_mes
  from
  conteo
  left join (
    select
    *
    from
    meses) b
    on conteo.td_id = b.td_id
    where conteo_mes > 1),

plata as (
  select 
  td_id,
  sum(money) money
  from
  base
  group by 1
)

select
conteo_total.td_id,
ordenes,
a.money
from
conteo_total 
left join(
  select
  *
  from
  plata
) a
on conteo_total.td_id = a.td_id 
order by ordenes desc



