drop table if exists reviews;

create table reviews as
select 
    asin 
    , reviewerID
    , reviewDate
from 'data/reviews/*.parquet';
