# Power BI Dashboard Plan for Job Seekers

## Goal

Build one Power BI report with four focused pages that help job seekers answer practical questions:

1. Where are the best job opportunities?
2. What salary should I expect?
3. Which skills should I learn or highlight?
4. Which jobs should I apply to first?

The target audience is people actively searching for jobs, so the report should prioritize decision-making over internal pipeline monitoring.

## Data sources from the Gold layer

Use the MotherDuck Gold schema as the Power BI source.

| Table | Role | Main use |
| --- | --- | --- |
| `gold.jobs` | Central fact table | Job title, company, location, salary, experience, source site, date key, job URL |
| `gold.dim_date` | Date dimension | Time trend based on crawl/partition date |
| `gold.job_industries` | Child table | One row per job and industry |
| `gold.job_benefits` | Child table | One row per job and benefit |
| `gold.job_requirements` | Child table | One row per job and requirement value, grouped by requirement type |
| `gold.dim_*_taxonomy` | Optional dimensions | Canonical labels and parent categories for skills, industries, benefits, languages, domains |

Important note: `date_key` is created from the Silver S3 partition date. In the report, call this **crawl date** or **data collection date**, not job posting date, unless a true posting date is added later.

## Page 1: Market Overview

### Purpose

Help job seekers understand the market size and where opportunities are concentrated.

### Key questions

- Which locations have the most jobs?
- Which industries are hiring the most?
- Which job positions are most available?
- Which source sites have the most listings?
- Is job availability changing over time?

### Visual layout

Top KPI row:

- Total Jobs
- Total Companies
- Average Salary
- Salary Coverage %

Main visuals:

1. Bar chart: `Total Jobs` by `clean_location`.
2. Bar chart: `Total Jobs` by `gold.job_industries[industry]`.
3. Column chart: `Total Jobs` by `job_position`.
4. Donut chart or bar chart: `Total Jobs` by `source_site`.
5. Line chart: `Total Jobs` by `gold.dim_date[full_date]`.
6. Table: latest or most relevant jobs with title, company, location, salary, source site, job URL.

### Recommended slicers

- Crawl date
- Location
- Industry
- Source site
- Job position
- Job type

### Design notes

- Put the trend chart near the top-right to show data freshness.
- Label the date slicer as `Crawl Date` or `Data Collection Date`.
- Use tooltips to show salary range and company size.

## Page 2: Salary Benchmark

### Purpose

Help job seekers benchmark salary expectations by role, experience, location, and industry.

### Key questions

- What is the typical salary for my target role?
- How does salary change by experience level?
- Which locations or industries pay better?
- Which jobs have high salary but reasonable experience requirements?

### Visual layout

Top KPI row:

- Average Salary
- Median Salary
- Jobs With Salary
- Salary Coverage %

Main visuals:

1. Bar chart: `Median Salary` by `clean_job_title`.
2. Bar chart: `Median Salary` by `clean_location`.
3. Box plot/custom visual or column chart: salary distribution by `Experience Band`.
4. Scatter plot:
   - X-axis: `Average Experience`
   - Y-axis: `Average Monthly Salary`
   - Size: `Total Jobs`
   - Legend: `job_position` or `clean_location`
5. Matrix: `job_position` x `Experience Band`, values = `Median Salary` and `Total Jobs`.
6. Table: high-paying jobs with title, company, location, salary range, experience range, job URL.

### Recommended slicers

- Job title
- Job position
- Location
- Industry
- Experience Band
- Source site

### Design notes

- Always show salary coverage because not every job has salary data.
- Prefer median salary over average salary for ranking because salary can have outliers.
- Use conditional formatting in the high-paying jobs table.

## Page 3: Skills Demand

### Purpose

Help job seekers decide which skills to learn, improve, or highlight on their CV.

### Key questions

- Which programming languages are most requested?
- Which frameworks, tools, and cloud skills are most valuable?
- Which skills are common in higher-paying jobs?
- Which skills are required for each target role?

### Visual layout

Top KPI row:

- Total Jobs
- Requirement Count
- Distinct Skills
- Average Salary

Create this measure for distinct skills:

```DAX
Distinct Skills =
DISTINCTCOUNT(gold.job_requirements[value])
```

Main visuals:

1. Bar chart: top `value` where `requirement_type = programming_language`.
2. Bar chart: top `value` where `requirement_type = framework`.
3. Bar chart: top `value` where `requirement_type = tool`.
4. Bar chart: top `value` where `requirement_type = cloud_skill`.
5. Matrix: `requirement_type` x `value`, values = `Total Jobs`.
6. Bar chart: `Median Salary` by skill, after joining/filtering through `job_url`.
7. Table: jobs matching selected skills.

### Recommended slicers

- Requirement type
- Skill value
- Job title
- Job position
- Location
- Industry
- Salary Band
- Experience Band

### Design notes

- This should be the strongest analytical page in the report.
- Separate skills by type instead of mixing all values in one chart.
- Use a tooltip page to show example jobs for a selected skill.
- If one job has multiple skills, use distinct job count rather than raw row count for most visuals.

Recommended measure for skill demand:

```DAX
Jobs Requiring Skill =
DISTINCTCOUNT(gold.job_requirements[job_url])
```

## Page 4: Job Finder and Recommendation

### Purpose

Turn the report into a practical job-search tool that helps users shortlist jobs.

### Key questions

- Which jobs best match my target location, salary, experience, and skills?
- Which jobs are beginner-friendly?
- Which jobs are high salary but do not require too much experience?
- Which jobs should I apply to first?

### Visual layout

Top KPI row:

- Total Jobs after filters
- Average Salary after filters
- Median Salary after filters
- Average Min Experience after filters

Main visuals:

1. Large searchable table with:
   - Clean job title
   - Company
   - Location
   - Salary range
   - Experience range
   - Job position
   - Source site
   - Job URL
2. Job detail card or multi-row card for the selected job.
3. Bar chart: selected jobs by source site.
4. Bar chart: selected jobs by location.
5. Bar chart: selected jobs by skills required.
6. Optional score table: recommended jobs sorted by fit score.

### Recommended slicers

- Location
- Salary Band
- Experience Band
- Job position
- Job type
- Industry
- Required skill
- Source site

### Simple recommendation score

Create a basic score that is easy to explain in a portfolio/demo. Adjust weights later if needed.

```DAX
Job Recommendation Score =
VAR SalaryScore =
    SWITCH(
        TRUE(),
        ISBLANK(gold.jobs[Average Monthly Salary]), 0,
        gold.jobs[Average Monthly Salary] >= 50000000, 4,
        gold.jobs[Average Monthly Salary] >= 30000000, 3,
        gold.jobs[Average Monthly Salary] >= 20000000, 2,
        1
    )
VAR ExperienceScore =
    SWITCH(
        TRUE(),
        ISBLANK(gold.jobs[min_exp_level]), 1,
        gold.jobs[min_exp_level] <= 1, 4,
        gold.jobs[min_exp_level] <= 3, 3,
        gold.jobs[min_exp_level] <= 5, 2,
        1
    )
RETURN
    SalaryScore + ExperienceScore
```

### Design notes

- This page should feel like a job search interface, not only a chart page.
- Keep the job table wide and readable.
- Add conditional formatting for salary and experience.
- Make `job_url` clickable if Power BI recognizes it as a web URL.

## Optional later page: Benefits and Company Attractiveness

Add this only after the four core pages are finished.

Useful visuals:

- Top benefits by number of jobs.
- Benefits by industry.
- Benefits by company size.
- Companies with the most benefits.
- Jobs with high salary and many benefits.

Main tables:

- `gold.jobs`
- `gold.job_benefits`
- `gold.job_industries`

## Build sequence

### Step 1: Connect Power BI to MotherDuck

Load these tables first:

- `gold.jobs`
- `gold.dim_date`
- `gold.job_industries`
- `gold.job_benefits`
- `gold.job_requirements`

Load taxonomy tables only if you need parent categories or English/Vietnamese canonical labels.

### Step 2: Clean model names

Rename tables in Power BI for readability:

- `jobs`
- `dim_date`
- `job_industries`
- `job_benefits`
- `job_requirements`

Set data categories:

- `job_url`: Web URL
- `full_date`: Date
- Salary fields: Whole number or decimal number, format as VND

### Step 3: Create relationships

Create the relationships listed in the recommended data model section.

Validate by selecting one job and checking that related industries, skills, and benefits filter correctly.

### Step 4: Create reusable measures

Create the calculated columns and measures before building visuals.

Use a separate measure table if you want the model to stay organized.

### Step 5: Build pages in this order

1. Market Overview
2. Salary Benchmark
3. Skills Demand
4. Job Finder and Recommendation

This order is best because each page reuses fields and measures from the previous pages.

### Step 6: Add interactions and drillthrough

Recommended interactions:

- Clicking a location filters all visuals on the same page.
- Clicking a skill filters the job table.
- Clicking an industry filters salary and skill visuals.
- Drillthrough from a job row to a job detail view if you add a dedicated detail page later.

### Step 7: Final quality checks

Before presenting the report, check:

- Job counts are distinct by `job_url`.
- Salary charts exclude blank salary values where appropriate.
- Date labels clearly say crawl/data collection date.
- Skill charts use distinct job count, not raw skill row count, unless the visual explicitly shows total requirement records.
- Slicers from child tables filter the job table correctly.
- Job URL links are clickable.
- Page titles and labels are written for job seekers, not data engineers.

## Suggested report story for presentation

Use this narrative when explaining the Power BI report:

> This report helps job seekers understand the hiring market, benchmark salary expectations, identify the most in-demand skills, and shortlist jobs that best match their target profile.

Recommended demo flow:

1. Start with Market Overview to show where the jobs are.
2. Move to Salary Benchmark to explain compensation expectations.
3. Move to Skills Demand to show what candidates should learn or highlight.
4. End with Job Finder to demonstrate practical job shortlisting.
