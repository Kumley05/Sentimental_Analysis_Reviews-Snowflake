create or replace table yelp_reviews( review_text variant);

copy into yelp_reviews
from 's3://project-nav-1/'
credentials = (
     AWS_Key_ID = 'Access_ID'
    AWS_SECRET_KEY = 'Secret_id'
)
FILE_FORMAT = (TYPE = JSON);

SELECT COUNT(*) FROM yelp_reviews;


create or replace table yelp_business (business_text variant);

copy into yelp_business
from 's3://project-nav-1/Business_review/'
credentials = (
    AWS_Key_ID = 'Access_ID'
    AWS_SECRET_KEY = 'Secret_id'
)
FILE_FORMAT = (TYPE = JSON);

SELECT * from yelp_business limit 100;


create table reviews (review varchar(200));

insert into reviews values('I love this product. It works prefectly!');
insert into reviews values('This product is okay but it could be better');
insert into reviews values('I hate this product it stopped working after a week.');
insert into reviews values('This product is okay. but not that great');
insert into reviews values('this product is not good but I can use');


select * from reviews;

create or replace function analyze_sentiment(text string)
returns string 
language python
runtime_version = '3.9'
packages = ('textblob')
handler = 'sentiment_analyer'
as $$
from textblob import TextBlob
def sentiment_analyer(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'

$$;

Select REVIEW,analyze_sentiment(REVIEW) from REVIEWS;

select * from yelp_business limit 10;


create or replace table tbl_yelp_business as
Select business_text:business_id::string as business_id,
business_text:name::string as business_name,
business_text:city::string city,
business_text:state::string state,
business_text:stars::number stars,
business_text:review_count::number review_count,
business_text:categories::string categories
from yelp_business 

create or replace table tbl_yelp_reviews as 
Select review_text:business_id::string business_id,
review_text:user_id::string user_id,
review_text:date::date review_date,
review_text:stars::number review_stars,
review_text:text::String review_comments,
analyze_sentiment(review_text) sentiments
from yelp_reviews ;


Select * from tbl_yelp_reviews limit 100;
select * from tbl_yelp_business limit 100;

select category, count(*)
from (Select business_id, trim(a.value) category
from tbl_yelp_business, lateral split_to_table(categories,',') a)
group by 1
order by 2 desc;


Select top 10 user_id, count(distinct a.business_id) count from tbl_yelp_reviews a
join tbl_yelp_business b on a.business_id=b.business_id 
where categories like '%Restaurants%'
group by user_id
order by 2 desc;


with cte as (
select business_id, trim(a.value) category
from tbl_yelp_business, lateral split_to_table(categories,',') a)
select category, count(*) no_of_category
from cte a
join tbl_yelp_reviews b on a.business_id = b.business_id 
group by category
order by 2 desc;


with cte2 as (
Select b.business_name, a.*,
row_number() over(partition by b.business_id order by review_date desc) rn
from tbl_yelp_reviews a
join tbl_yelp_business b on a.business_id = b.business_id
qualify row_number() over(partition by b.business_id order by review_date desc) <=3)
select * 
from cte2
-- where rn <=3
-- qualify row_number() over(partition by b.business_id order by review_date desc) <=3
;

Select month(review_date) month, count(*) no_of_review
from tbl_yelp_reviews
group by 1
order by 2 desc;


-- with fiveStar as (
-- Select business_id ,count(*) no_of_fiveStarreviews
-- from tbl_yelp_business
-- where review_count = 5
-- group by business_id),
-- reviewCount as (
-- select business_id, count(*) no_of_reviews
-- from tbl_yelp_reviews
-- group by business_id
-- )
-- Select a.business_id, round(no_of_fiveStarreviews/no_of_reviews *100,2) per
-- from reviewcount a 
-- join fivestar b on a.business_id = b.business_Id


Select a.business_id, a.business_name, count(*) no_of_reviews,
sum(case when b.review_stars = 5 then 1 else 0 end) star5_reviews,
round(star5_reviews/no_of_reviews *100,2) per
from tbl_yelp_business a 
join tbl_yelp_reviews b on a.business_id =b.business_id
group by a.business_id, a.business_name



