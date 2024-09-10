-- SQL Data cleaning


select *
FROM layoffs;

-- 1. Removing Duplicates
-- 2. Standardize the data
-- 3. NULL values or blank values
-- 4. Remove uncessary columns

-- Creating a staging table to work on
CREATE TABLE layoffs_staging_db
LIKE layoffs;

select *
FROM layoffs;


INSERT layoffs_staging_db
SELECT *
FROM layoffs;

-- Finding the duplicates
-- Using a workaround method since there was no unique ID

WITH duplicate_cte AS (
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) as row_num 
FROM layoffs_staging_db
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging_db
where company = 'Casper';


CREATE TABLE `layoffs_staging_db2` (
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

SELECT *
FROM layoffs_staging_db2;


Insert into layoffs_staging_db2
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) as row_num 
FROM layoffs_staging_db;

-- Identifying the duplicates

SELECT *
FROM layoffs_staging_db2
where row_num > 1;

-- Deleting the duplicates
DELETE
FROM layoffs_staging_db2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging_db2;


-- Standadizing data
-- Finding issues in the data and fixing them

select company, TRIM(company)
FROM layoffs_staging_db2;

-- Removing the white spaces at the end of data

Update layoffs_staging_db2  
SET company = TRIM(company); 

select company, TRIM(company)
FROM layoffs_staging_db2;


select *
From layoffs_staging_db2
Where industry like 'Crypto%';

update layoffs_staging_db2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
FROM layoffs_staging_db2
order by 1;


update layoffs_staging_db2
set country = trim(trailing '.' from country)
where country like 'United States%';

select date,
str_to_date(date, '%m/%d/%Y')
FROM layoffs_staging_db2;

update layoffs_staging_db2
set date = str_to_date(date, '%m/%d/%Y');


Alter table layoffs_staging_db2
modify column date Date;

-- Removing Nulls and blanks

select *
FROM layoffs_staging_db2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging_db2
set industry = null
where industry = '';

select distinct industry
FROM layoffs_staging_db2
where industry is null
or industry = '';

select *
FROM layoffs_staging_db2
where company = 'Airbnb';

select *
from layoffs_staging_db2 t1
join layoffs_staging_db2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


update layoffs_staging_db2 t1
join layoffs_staging_db2 t2
	on t1.company = t2.company
    set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


select *
FROM layoffs_staging_db2;

delete 
from layoffs_staging_db2
where total_laid_off is null
and percentage_laid_off is null;


-- Removing uncessary columns or rows

alter table layoffs_staging_db2
drop column row_num;

















