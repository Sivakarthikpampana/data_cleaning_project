select * from project;

create table project_update1
like project;

insert project_update1 select * from project;

select * from project_update1;


-- step 1: remove duplicates

select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
 funds_raised_millions)
as row_num
from project_update1;


with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
 funds_raised_millions)
as row_num
from project_update1
)
select * from duplicate_cte where row_num>1;

select * from project_update1
where company='yahoo';

CREATE TABLE `project_update2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from project_update2;

insert into project_update2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
 funds_raised_millions)
as row_num
from project_update1;

select * from project_update2
where row_num>1;

delete from project_update2
where row_num>1;

-- step 2: standardize the data
select distinct(company) from project_update2;

select distinct(company) ,trim(company) as companY
from project_update2;

update project_update2 set
company=trim(company)
;
select * from project_update2;

select distinct(industry) from project_update2
order by 1;

select * from project_update2
where industry like 'crypto%';
 
 update project_update2
 set industry='Crypto'
 where industry like'crypto%';
 
select * from project_update2
;

select distinct(industry) from project_update2;

select distinct location from project_update2 order by 1;
select distinct country from project_update2 order by 1;

select distinct country from project_update2
where country like 'united s%';

update project_update2
set country='united states'
where country like 'united s%'
;

select `date` from project_update2;

select `date` ,
str_to_date(`date`,'%m/%d/%Y') as DATE
from project_update2;

update project_update2
set date =str_to_date(`date`,'%m/%d/%Y') ;

select `date` from project_update2;

alter table project_update2
modify column `date` date;

-- step 3: remove null or blank values

select * from project_update2
where total_laid_off is null;

select * from project_update2
where total_laid_off is null
and percentage_laid_off is null;

select distinct industry from project_update2 where industry is null or industry='';

update project_update2
set industry=null
where industry ='';

select * from project_update2 where industry is null or industry='';
 
 select * from project_update2 p1
 join project_update2 p2
 on p1.company=p2.company
 where(p1.industry is null or p1.industry='')
 and p2.industry is not null;
 
  select p1.industry,p2.industry
  from project_update2 p1
 join project_update2 p2
 on p1.company=p2.company
 where(p1.industry is null or p1.industry='')
 and p2.industry is not null;
 
 update project_update2 p1
 join project_update2 p2
 on p1.company=p2.company
 set p1.industry=p2.industry
 where(p1.industry is null or p1.industry='')
 and p2.industry is not null;
 
 select * from project_update2
 where company='airbnb';
 
 select * from project_update2
where total_laid_off is null
and percentage_laid_off is null;

delete from project_update2
where total_laid_off is null
and percentage_laid_off is null;

-- step 4: remove unwanted columns

alter table project_update2 drop column row_num;

select * from project_update2;