drop table if exists user_lookup;
drop table if exists item_lookup;

create temporary table unique_users as
    select
        distinct reviewerID
        from reviews;


create table user_lookup as
select 
    reviewerID
,   row_number() over (order by random()) -1 as user_id

from unique_users;


create temporary table unique_items as
    select
        distinct asin
        from meta;


create table item_lookup as
select 
    asin
,   row_number() over (order by random()) -1 as item_id

from unique_items;
