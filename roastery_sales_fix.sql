drop database maven_roastery;
create database if not exists maven_roastery;
use maven_roastery;

create table maven_roastery_sales
(transaction_id	varchar (50),
transaction_date date,
transaction_time time,
transaction_qty	int,
store_id int,
store_location varchar (255),
product_id int,
unit_price double,
product_category varchar (255),
product_type varchar (255),
product_detail varchar (255));

alter table maven_roastery_sales
modify column transaction_id varchar (255);

select *
from maven_roastery_sales;

show variables like	'secure_file_priv';

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/roastery_sales.csv' 
into table maven_roastery_sales
fields terminated by ';'
ignore 1 lines;  

select *
from maven_roastery_sales
limit 10;

create table maven_roastery_sales_1
like maven_roastery_sales;

insert maven_roastery_sales_1
select *
from maven_roastery_sales;

## remove duplicate
with dc As
(select*,
row_number() over(
partition by transaction_id, transaction_date, transaction_time, transaction_qty, 
store_id, product_id, unit_price, product_category,
product_type, product_detail) As row_num
from maven_roastery_sales_1)
select *
from dc
where row_num > 1;

## remove blank cell and null
select *
from maven_roastery_sales_1
where product_type is null or product_category is null
or unit_price is null or product_detail is null;

select *
from maven_roastery_sales_1
where product_type = '' or product_category = '' 
or unit_price = '' or product_detail = '';

## standardizing data
select store_location, product_category, product_type, product_detail,
trim(store_location), trim(product_category), trim(product_type), trim(product_detail)
from maven_roastery_sales_1; 

update maven_roastery_sales_1
set store_location = trim(store_location), product_category = trim(product_category),
product_type = trim(product_type), product_detail = trim(product_detail);

select count(distinct product_type), count(distinct product_category), count(distinct product_detail),
count(distinct unit_price), count(distinct product_id)
from maven_roastery_sales_1;

select max(transaction_date), max(transaction_time), max(unit_price), max(transaction_qty),
min(transaction_date), min(transaction_time), min(unit_price), min(transaction_qty),
sum(unit_price), sum(transaction_qty), avg(unit_price), avg(transaction_qty)
from maven_roastery_sales_1; 

### data transformation
## transform date to day of the week
alter table maven_roastery_sales_1
add column day_of_week varchar(20);

update maven_roastery_sales_1
set day_of_week = dayname(transaction_date);

## extract month
alter table maven_roastery_sales_1
add column `month` varchar(20);

update maven_roastery_sales_1
set `month` = monthname(transaction_date);

## transform time to 24 hours
alter table maven_roastery_sales_1
add column 24_hours time;

update maven_roastery_sales_1
set 24_hours = time(sec_to_time(round(time_to_sec(transaction_time)/ 3600)*3600)); 

### Analysis
## 3 Best Sold Product Category Based on Store Location
with bspc AS (select store_location, product_category, 
sum(transaction_qty),
	row_number() over(
	partition by store_location 
	order by sum(transaction_qty) desc) AS popularity
	from maven_roastery_sales_1
	group by store_location, product_category)
select *
from bspc
where popularity <= 3;

## Product Category Best Performing Sales Between 3 Store
select *
from (select product_category, sum(transaction_qty), store_location,
		row_number() over(
        partition by product_category
        order by sum(transaction_qty) desc) As `rank` 
		from maven_roastery_sales_1
        group by store_location, product_category) As pcb
where `rank` <= 1;

select store_location, count(store_location)
from (select product_category, sum(transaction_qty), store_location,
		row_number() over(
        partition by product_category
        order by sum(transaction_qty) desc) As `rank` 
		from maven_roastery_sales_1
        group by store_location, product_category) As pcb
where `rank` <= 1
group by store_location;

## 3 Best Sold Product Type Based on Store Location
with bspt As	(select store_location, product_type, sum(transaction_qty),
				row_number() over(
				partition by store_location
				order by sum(transaction_qty) desc) As popularity
				from maven_roastery_sales_1
				group by product_type, store_location)                
select *
from bspt
where popularity <= 3;

## average price of product category vs unit price of product type
with apt As	(select store_location, product_type, product_category,
				unit_price,
				row_number() over(
				partition by store_location)
				from maven_roastery_sales_1
				group by product_type, store_location, product_category, unit_price),
	pcm As	(select product_category, avg(unit_price) As aup,
				row_number() over(
				partition by product_category)
				from maven_roastery_sales_1
				group by product_category)
select store_location, product_type, apt.product_category, unit_price, 
round(aup, 2) As aup,
case
	when unit_price > aup then 'no'
    else 'yes'
end As deals
from apt right join pcm on apt.product_category = pcm.product_category; 	

## count number of product type with lower than avg product category by store location
with price As	(select distinct product_category, 
				store_location, unit_price
				from maven_roastery_sales_1),
	 cat_avg As (select product_category, avg(unit_price) As avg_price
				 from price
                 group by product_category)
select price.product_category, store_location, count(*)
from price
join cat_avg on price.product_category = cat_avg.product_category
where price.unit_price < cat_avg.avg_price
group by store_location, price.product_category;

## product detail which are higher than avg product category
with pdd As (select distinct product_category, product_type, product_detail, unit_price
			from maven_roastery_sales_1),
	 ap  As (select product_category, avg(unit_price) As avg_price
			from pdd
            group by product_category)
select product_type, product_detail
from pdd
join ap on pdd.product_category = ap.product_category
where pdd.unit_price > ap.avg_price
group by product_type, product_detail;

## percentage of product detail which are higher than avg product category
with pdd As (select distinct product_category, product_type, product_detail, unit_price
			from maven_roastery_sales_1),
	 ap  As (select product_category, avg(unit_price) As avg_price
			from pdd
            group by product_category)
select count(*)/ (select count(distinct product_detail) from maven_roastery_sales_1 ) * 100
from pdd
join ap on pdd.product_category = ap.product_category
where pdd.unit_price > ap.avg_price;

## top 3 most profitable products based on store
select product_type, product_detail, round(sum(unit_price), 2) As sup
from maven_roastery_sales_1
group by product_type, product_detail
order by sup desc;

with mpp As		(select store_location, product_detail, round(sum(unit_price), 2) As revenue,
				row_number() over(
				partition by store_location
				order by round(sum(unit_price), 2) desc) As rank_profit
				from maven_roastery_sales_1
				group by store_location, product_detail)                
select *
from mpp
where rank_profit <= 3;

## store outlook
select store_location, round(sum(unit_price), 2) As revenue, sum(transaction_qty) As sales
from maven_roastery_sales_1
group by store_location;

## most profitable store month by month
with bsmm As	(select store_location, `month`, round(sum(unit_price), 2) As revenue,
				row_number() over(
				partition by `month`
				order by round(sum(unit_price), 2) desc) As revenue_rank
				from maven_roastery_sales_1
				group by store_location, `month`)                
select *
from bsmm
where revenue_rank = 1;

## highest sales store month by month
with bsmm As	(select store_location, `month`, sum(transaction_qty) As sales,
				row_number() over(
				partition by `month`
				order by sum(transaction_qty) desc) As sales_rank
				from maven_roastery_sales_1
				group by store_location, `month`)                
select *
from bsmm
where sales_rank = 1;

## observation
select max(transaction_qty)
from maven_roastery_sales_1;

select min(transaction_qty)
from maven_roastery_sales_1;

select avg(transaction_qty)
from maven_roastery_sales_1;

select max(unit_price)
from maven_roastery_sales_1;

select min(unit_price)
from maven_roastery_sales_1;

select round(avg(unit_price))
from maven_roastery_sales_1;

## highest performing month
select `month`, round(sum(unit_price), 2) As revenue
from maven_roastery_sales_1
group by `month`
order by revenue desc
limit 1;

select `month`, count(transaction_qty) As ctq
from maven_roastery_sales_1
group by `month`
order by ctq desc;

## highest performing day
select day_of_week, round(sum(unit_price), 2) As revenue
from maven_roastery_sales_1
group by day_of_week
order by revenue desc
limit 1;

select day_of_week, count(day_of_week) As cdw
from maven_roastery_sales_1
group by day_of_week
order by cdw desc;

## highest performing date
select transaction_date, round(sum(unit_price), 2) As revenue
from maven_roastery_sales_1
group by transaction_date
order by revenue desc;

select transaction_date, count(transaction_date) As ctd
from maven_roastery_sales_1
group by transaction_date
order by ctd desc;

## highest performing hour
select `24_hours`, round(avg(unit_price), 2) As revenue
from maven_roastery_sales_1
group by `24_hours`
order by revenue desc;

select `24_hours`, count(`24_hours`) As c24h
from maven_roastery_sales_1
group by `24_hours`
order by c24h desc;

describe maven_roastery_sales_1;

