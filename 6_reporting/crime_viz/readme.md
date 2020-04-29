
# Story: Does Covid-19 affect crime rate?
- [Crime rates drop across the nation amid coronavirus](https://thehill.com/homenews/state-watch/491055-crime-rates-drop-across-the-nation-amid-coronavirus)
- [Greenville sees a drop in its crime rate due to coronavirus](https://www.witn.com/content/news/Greenville-sees-a-drop-in-its-crime-rate-due-to-coronavirus-569840881.html)



# Datasets: 
## Covid 
- [nytimes](https://github.com/nytimes/covid-19-data)

## Crime data
- [Atlanta](https://www.atlantapd.org/Home/ShowDocument?id=3209)
- [Austin](https://data.austintexas.gov/Public-Safety/Crime-Reports/fdj4-gpfu)
- [Chicago](https://data.cityofchicago.org/api/views/x2n5-8w5q/rows.csv)
- [Denver](https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv)
- [Los Angeles](https://data.lacity.org/api/views/2nrs-mtv8/rows.csv)
- [Marylan](https://data.montgomerycountymd.gov/api/views/icn6-v9z3/rows.csv)
- [Phoenix](https://www.phoenixopendata.com/dataset/cc08aace-9ca9-467f-b6c1-f0879ab1a358/resource/0ce3411a-2fc6-4302-a33f-167f68608a20/download/crimestat.csv)

-----

Initilal visualization for Chicgao: 

![Initial_Chicago_Viz](https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/7_Assets/img//Tableau%20-%20alexw-crime-covid%202020-04-24%2023-06-59.png?raw=true)

------

# ETL 

## Pentaho Job
    1. Getting most updated data from nytimes. 
    2. SQL generate US Daily New Cases Summary

![pentaho_job.png](https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/7_Assets/img//pentaho_job.png?raw=true)

## Tableau Prep
  - pro: 
    - tableau prep provided fast cleaning results preview. cleaning/simple UI comparing to pentaho. 
    - ability to connect with R or Python for advanced cleaning/manipulation
  - Cons:
    - Features are limited comparing to pentaho, for example it dosen't allow user to output table to database. 
    - [Java heap space](https://kb.tableau.com/articles/issue/error-system-error-java-heap-space-when-exporting-an-output-or-running-a-tableau-prep-flow) problem when dataset is too large 

## Code Example / Cleaning Flow: 
link to the script used in the flow: https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/5_ETL/tabpy_pull_crimes.py
![tabpy_prep_pull_crimes_flow](https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/7_Assets/img//crime_prep_flow.png?raw=true)


## Crime Data Cleaning Stage 1: getting data
we only needed three attribute `date_of_occurrence`, `categorry` and `city`


![pulling_functions](https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/7_Assets/img//tabpy.png?raw=true)



## Crime Data Cleaning Stage 2: category regrouping/replacing
![regrouping_prep.png](https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/7_Assets/img//regrouping_prep.png?raw=true)

```Last step was to union crime data and US corona new cases together so we can compare them. ```

## Visualization 
[Tableau Public](https://public.tableau.com/views/ytd_crime/Dashboard2?:display_count=y&publish=yes&:origin=viz_share_link)

Video Preview
[![](https://github.com/Illinois-Tech-Projects/Covid-Data-Warehousing-ETL/blob/master/7_Assets/img//crime_vs_corona.png?raw=true)](https://i.imgur.com/5vq01cA.mp4)

