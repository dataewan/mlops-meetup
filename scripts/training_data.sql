select
     user_id
    ,item_id

from
reviews join user_lookup
    on reviews.reviewerID = user_lookup.reviewerID
join item_lookup
    on reviews.asin = item_lookup.asin

group by user_id, item_id
