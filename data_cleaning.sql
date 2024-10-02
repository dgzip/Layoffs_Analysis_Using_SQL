-- Data Cleaning

SELECT * 
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove any columns or rows


-- 0. Create staging table

CREATE TABLE layoffs_staging
LIKE layoffs;


INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

SELECT * FROM layoffs_staging;


-- 1. Removing duplicates

-- Creating row number to identify duplicate columns
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) row_num
FROM layoffs_staging;


-- Finding duplicates
WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

-- Checking whether the result returned actually have duplicate rows
SELECT *
FROM layoffs_staging
WHERE company='Hibob';

-- To delete these duplicate rows we create another staging table with the row number and then delete
-- those rows having row_num>1

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

-- Inserting data into layoffs_staging2
INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) row_num
FROM layoffs_staging;


-- Deleting duplicates
DELETE
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;

-- 2. Standardize the data

-- Trimming spaces in company column
SELECT company,TRIM(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT * 
FROM layoffs_staging2;

-- Take a look at industry column
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Change crypto column naming difference to a standard name
UPDATE layoffs_staging2 
SET industry="Crypto"
WHERE industry LIKE "Crypto%";

SELECT *
FROM layoffs_staging2;

-- Take a look at location column
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- Take a look at country column
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- Fix the 'united states.' rows
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Fix the date column(convert from text to datetime)
SELECT `date`
FROM layoffs_staging2;

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- Still the date column is text, but now we can convert to date datatype, earlier we couldn't
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- 3. Null values and blank values

-- Looking at the null and blank values across columns
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry='';

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb';


-- Find industry of companies which are given as null or blank 
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company=t2.company
WHERE (t1.industry IS NULL)
AND t2.industry	IS NOT NULL;

-- Convert blanks into null to facilitate update
UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';

-- Update the industry
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry	IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry='';

SELECT *
FROM layoffs_staging2;

-- Deleting rows having both total and percentage laid off as null
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- 4. Remove any columns or rows

-- Deleting column row num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

