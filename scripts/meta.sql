drop table if exists meta;

create table meta as
select 
asin 
,title
from 'data/meta/*.parquet'
