# Advanced Data Cleaning with SQL
This project focuses on using advanced SQL techniques to clean and prepare a large dataset. The primary goal was to identify and resolve duplicates, inconsistencies, and missing values, ensuring the data’s accuracy and reliability for analysis.

### Data Cleaning Techniques
This project applied several advanced SQL techniques to ensure high-quality data:

- Duplicate Resolution: Used window functions and CTEs to identify and remove duplicate records while retaining the most accurate data.
- Inconsistency Correction: Implemented conditional logic (CASE statements) and joins to correct inconsistent data values.
- Handling Missing Values: Applied NULL handling techniques to fill or impute missing data based on trends and logical substitutions.
### Sample Queries
Some example queries used in this project include:

- Duplicate Identification and Removal:
  `WITH duplicate_cte AS (
select *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) as row_num 
FROM layoffs_staging_db
)`

- Inconsistency Correction:
`update layoffs_staging_db2
set industry = 'Crypto'
where industry like 'Crypto%';
`

- Handling Missing Values:
  `update layoffs_staging_db2 t1
join layoffs_staging_db2 t2
	on t1.company = t2.company
    set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;
`
