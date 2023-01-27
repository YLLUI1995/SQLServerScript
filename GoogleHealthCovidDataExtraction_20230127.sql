
----- Public Source Data: https://health.google.com/covid-19/open-data/raw-data
----- Files: index.csv, geography.csv, demographics.csv, epidemiology.csv
----- Downloaded on: 01/24/2023
----- Written by YLLUI1995 on 01/26/2023

------------ Preparation: In SSIS: import 4 csv files into SQL Server (Note: all fields were imported as varchar(50))
select * from tblIndex           --22,963 recordes
select * from tblGeography       --22,130 recordes
select * from tblDemographics    --21,689 recordes

select count(*) from tblEpidemiology --12,525,825 
select top 1000 * from tblEpidemiology 

------------ Step 1: In SQL Server: create indexes on tables
create index tblIndex_Index_location_key on tblIndex (location_key)
create index tblGeography_Index_location_key on tblGeography (location_key)
create index tblDemographics_Index_location_key on tblDemographics (location_key)
create index tblEpidemiology_Index1_location_key on tblEpidemiology (location_key)


------------ Step 2: create a helper function: 
--                   if value passed contains an integer value: return its value as bigint, 
--                   else: return 0 (i.e., Empty record)
create or alter function fncZeroWhenEmptyOrNotBigInt (@bigint varchar(50)) returns bigint
as
begin
    if (@bigint is null or 
	    ltrim(rtrim(@bigint))='' or  
	    case when @bigint not like '%[^0-9]%' then 'bigint' else 'Not a bigint' end <> 'bigint'
	   )
	   return 0  
	return convert(int, @bigint) 
end
go
grant exec on fncZeroWhenEmptyOrNotBigInt to public

---- testing function fncZeroWhenEmptyOrNotInt
--select dbo.fncZeroWhenEmptyOrNotInt('12')    --return 12
--select dbo.fncZeroWhenEmptyOrNotInt(null)    --return 0
--select dbo.fncZeroWhenEmptyOrNotInt('0.33')  --return 0
--select dbo.fncZeroWhenEmptyOrNotInt('9')     --return 9
--select dbo.fncZeroWhenEmptyOrNotInt('  ')    --return 0




------------ Step 3:  create view vwIndexGeographyDemographics 
--                    a) convert varchar(50) on numeric fields to bigint
--                    b) find out records' aggregate_level (country/state/count)
--                    c) add 2 extra new fields  
--                    d) rename some fields  
create or alter view vwIndexGeographyDemographics as 
 select case when subregion1_code='' then 'country'
            when subregion1_code<>'' and subregion2_name=''  then 'state'
		    when subregion1_code<>'' and subregion2_name<>''  then 'county'
	   end as aggregate_level, 
       i.location_key, 
       i.country_name, i.subregion1_code as state, i.subregion2_name as county,
	   g.latitude, g.longitude,
       dbo.fncZeroWhenEmptyOrNotBigInt(population) as population,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_female) as population_female,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_male) as population_male,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_00_09) as population_age_00_09,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_10_19) as population_age_10_19,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_20_29) as population_age_20_29,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_30_39) as population_age_30_39,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_40_49) as population_age_40_49,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_50_59) as population_age_50_59,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_60_69) as population_age_60_69,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_70_79) as population_age_70_79,
       dbo.fncZeroWhenEmptyOrNotBigInt(population_age_80_and_older) as population_age_80_and_older,
       (dbo.fncZeroWhenEmptyOrNotBigInt(population_age_60_69) + 
	    dbo.fncZeroWhenEmptyOrNotBigInt(population_age_70_79) +  
	    dbo.fncZeroWhenEmptyOrNotBigInt(population_age_80_and_older)) as population_senior, 
	   cast( 
			 ( dbo.fncZeroWhenEmptyOrNotBigInt(population_age_60_69) + 
			   dbo.fncZeroWhenEmptyOrNotBigInt(population_age_70_79) +  
			   dbo.fncZeroWhenEmptyOrNotBigInt(population_age_80_and_older)
			 )*1.0000/dbo.fncZeroWhenEmptyOrNotBigInt(population)
			 AS numeric(18,4)
		   ) as population_senior_percentage                
from tblIndex i
inner join tblGeography g on g.location_key=i.location_key
inner join tblDemographics d on d.location_key=i.location_key
where dbo.fncZeroWhenEmptyOrNotBigInt(population)>0 and 
      dbo.fncZeroWhenEmptyOrNotBigInt(population_age_60_69)>0 and 
      dbo.fncZeroWhenEmptyOrNotBigInt(population_age_70_79)>0 and
	  dbo.fncZeroWhenEmptyOrNotBigInt(population_age_80_and_older)>0 

------------ use vwIndexGeographyDemographics to get data on specific aggregate level 
-- country level
 select * from vwIndexGeographyDemographics where aggregate_level='country' order by country_name

-- state level
 select * from vwIndexGeographyDemographics where aggregate_level='state' order by country_name, state

 -- county level
 select * from vwIndexGeographyDemographics where aggregate_level='county' order by country_name, state, county


------------ Step 4: investigate table tblEpidemiology, delete "EMPTY" records
--                  (i.e., no value in new_confirmed/new_deceased/new_recovered/new_tested columns
select * 
from tblEpidemiology 
where dbo.fncZeroWhenEmptyOrNotInt(new_confirmed)=0 and
      dbo.fncZeroWhenEmptyOrNotInt(new_deceased)=0 and
      dbo.fncZeroWhenEmptyOrNotInt(new_recovered)=0 and
      dbo.fncZeroWhenEmptyOrNotInt(new_tested)=0 

------ delete 4,104,945 "EMPTY" rows
delete
from tblEpidemiology 
where dbo.fncZeroWhenEmptyOrNotInt(new_confirmed)=0 and
      dbo.fncZeroWhenEmptyOrNotInt(new_deceased)=0 and
      dbo.fncZeroWhenEmptyOrNotInt(new_recovered)=0 and
      dbo.fncZeroWhenEmptyOrNotInt(new_tested)=0 


------------ Step 5: create view vwEpidemiology storing fact Epidemiology, including all aggregate levels
create or alter view vwEpidemiology as 
 select 
       case when subregion1_code='' then 'country'
            when subregion1_code<>'' and subregion2_name=''  then 'state'
		    when subregion1_code<>'' and subregion2_name<>''  then 'county'
	   end as aggregate_level, 
       i.location_key, 
	   convert(date, [date]) as date,
       i.country_name, i.subregion1_code as state, i.subregion2_name as county, 
	   dbo.fncZeroWhenEmptyOrNotInt(new_confirmed) as new_confirmed,  
	   dbo.fncZeroWhenEmptyOrNotInt(new_deceased) as new_deceased,
	   dbo.fncZeroWhenEmptyOrNotInt(new_recovered) as new_recovered,
	   dbo.fncZeroWhenEmptyOrNotInt(new_tested) as new_tested
from tblEpidemiology 
inner join tblIndex i on i.location_key=tblEpidemiology.location_key
where new_confirmed>0 or new_deceased>0 or new_tested>0

------------ Step 5A: create view vwEpidemiology storing fact Epidemiology, on COUNTRY level only
create or alter view vwEpidemiologySummaryByCountry as 
select location_key, 
       country_name, 
       sum(new_confirmed) as new_confirmed_sum,
	   sum(new_deceased) as new_deceased_sum,
	   sum(new_tested) as new_tested_sum,
	   CAST(sum(new_deceased)*1.0000/sum(new_confirmed) AS NUMERIC(18,4)) 
	                as deceased_percentage_over_confirmed
from vwEpidemiology
where aggregate_level='country'
group by country_name, location_key
having sum(new_confirmed) >0
go

------------ Step 5B: create view vwEpidemiology storing fact Epidemiology, on STATE level only
create or alter view vwEpidemiologySummaryByState as 
select location_key, 
       country_name, 
	   state,
       sum(new_confirmed) as new_confirmed_sum,
	   sum(new_deceased) as new_deceased_sum,
	   sum(new_tested) as new_tested_sum,
	   CAST(sum(new_deceased)*1.0000/sum(new_confirmed) AS NUMERIC(18,4)) 
	                as deceased_percentage_over_confirmed
from vwEpidemiology
where aggregate_level='state'
group by country_name, state, location_key
having sum(new_confirmed) >0
go

------------ Step 5C: create view vwEpidemiology storing fact Epidemiology, on COUNTY level only
create or alter view vwEpidemiologySummaryByCounty as 
select location_key, 
       country_name, 
	   state,
	   county,
       sum(new_confirmed) as new_confirmed_sum,
	   sum(new_deceased) as new_deceased_sum,
	   sum(new_tested) as new_tested_sum,
	   CAST(sum(new_deceased)*1.0000/sum(new_confirmed) AS NUMERIC(18,4)) 
	                as deceased_percentage_over_confirmed
from vwEpidemiology
where aggregate_level='county'
group by country_name, state, county, location_key
having sum(new_confirmed) >0


----- use views by level of country/state/county, order by new_confirmed_sum
select * from vwEpidemiologySummaryByCountry order by new_confirmed_sum desc
select * from vwEpidemiologySummaryByState where country_name='United States of America' order by new_confirmed_sum desc
select * from vwEpidemiologySummaryByCounty where country_name='United States of America' and state='AZ' order by new_confirmed_sum desc

----- use views by level of country/state/county, order by deceased_percentage_over_confirmed desc
select * from vwEpidemiologySummaryByCountry order by deceased_percentage_over_confirmed desc
select * from vwEpidemiologySummaryByState where country_name='United States of America' order by deceased_percentage_over_confirmed desc
select * from vwEpidemiologySummaryByCounty where country_name='United States of America' and state='AZ' order by deceased_percentage_over_confirmed desc


------------ Step 6: create stored procedure procGetCovidData with parameters and output aggregated covid data along with demographic and geographic data
------------         (can be used directly from Power BI, Tableau, SSRS, etc)
create or alter procedure dbo.procGetCovidData
(@aggregate_level varchar(10) = '', -- valid values: country / state / county
 @country_name varchar(50) = '',    -- valid values: empty (i.e., all countries) / United States of America / Canada / ...
 @state varchar(50)='',             -- valid values: empty (i.e., all states) / AZ / CA / ...
 @county varchar(50)='')            -- valid values: empty (i.e., all counties) / Pima County / ...
as
if @aggregate_level='country'
  begin
     select s.location_key,
       s.country_name,
       new_confirmed_sum, new_deceased_sum, deceased_percentage_over_confirmed,
	   latitude, longitude,
       population, population_senior, population_senior_percentage
	 from vwEpidemiologySummaryByCountry s
	 inner join vwIndexGeographyDemographics i on i.location_key=s.location_key
	 where @country_name='' or s.country_name=@country_name  --either all countries, or specific country
	 order by new_confirmed_sum desc
  end


if @aggregate_level='state'
  begin
     select s.location_key,
       s.country_name,s.state,
       new_confirmed_sum, new_deceased_sum, deceased_percentage_over_confirmed,
	   latitude, longitude,
       population, population_senior, population_senior_percentage
	from vwEpidemiologySummaryByState s
	inner join vwIndexGeographyDemographics i on i.location_key=s.location_key
	where (@country_name='' or s.country_name=@country_name) and  --either all countries, or specific country
	      (@state='' or s.state=@state)                           --either all states, or specific state
	order by s.country_name asc, new_confirmed_sum desc
  end

  
if @aggregate_level='county'
  begin
     select s.location_key,
       s.country_name,s.state,s.county,
       new_confirmed_sum, new_deceased_sum, deceased_percentage_over_confirmed,
	   latitude, longitude,
       population, population_senior, population_senior_percentage
	from vwEpidemiologySummaryByCounty s
	inner join vwIndexGeographyDemographics i on i.location_key=s.location_key
	where (@country_name='' or s.country_name=@country_name) and  --either all countries, or specific country
	      (@state='' or s.state=@state) and                       --either all states, or specific state
		  (@county='' or s.county=@county)                        --either all counties, or specific county
	order by s.country_name asc, s.state asc, new_confirmed_sum desc
  end
  go
  grant exec on dbo.procGetCovidData to public

  -------- testing stored procedure procGetCovidData
  --get all countries data on country level
  exec dbo.procGetCovidData @aggregate_level='country', @country_name='', @state=''

  --return United States of America's data on country level  (one record returned)
  exec dbo.procGetCovidData @aggregate_level='country', @country_name='United States of America', @state=''

  --return United States of America's data on state level 
  exec dbo.procGetCovidData @aggregate_level='state', @country_name='United States of America'

  --return United States of America/AZ data on state level (one record returned)
  exec dbo.procGetCovidData @aggregate_level='state', @country_name='United States of America',  @state='AZ'

  --return United States of America/AZ data on county level 
  exec dbo.procGetCovidData @aggregate_level='county', @country_name='United States of America',  @state='AZ'

  --return United States of America/AZ/Pima County data on county level (one record returned)
  exec dbo.procGetCovidData @aggregate_level='county', @country_name='United States of America',  @state='AZ', @county='Pima County'


------------ Step 7: Some SQL Script for Reporting (Power BI, Tableau, SSRS, etc)
---- 7A: all data on country level
exec dbo.procGetCovidData @aggregate_level='country', @country_name='', @state='' --226 records

---- 7B: all data on state level
exec dbo.procGetCovidData @aggregate_level='state'  --798 records

---- 7C: all data on county level
exec dbo.procGetCovidData @aggregate_level='county'  -- 14417 records

---- 7D: all state data for US, on state level
select * 
from vwEpidemiology
where aggregate_level='state'
and country_name='United States of America'


---- 7E: Monthly Average New Cases in US, on country level
select country_name, 
       year(date) as year, 
       month(date) as month_number, 
	   datename(month, date) as month_name, 
	   sum(new_confirmed) as sum_new_confirmed, 
	   sum(new_deceased) as sum_new_deceased
from vwEpidemiology
where aggregate_level='country' and
      country_name='United States of America'
group by country_name, year(date), month(date), datename(month, date)
order by year, month_number


---- 7F: aggregate covid data by state in US: calculate deceased_percentage_over_confirmed
select location_key, 
       country_name, 
	   state,
       sum(new_confirmed) as new_confirmed_sum,
	   sum(new_deceased) as new_deceased_sum,
	   sum(new_tested) as new_tested_sum,
	   CAST(sum(new_deceased)*1.0000/sum(new_confirmed) AS NUMERIC(18,4)) 
	            as deceased_percentage_over_confirmed
from vwEpidemiology
where aggregate_level='state'
and country_name='United States of America'
group by country_name, state, location_key
having sum(new_confirmed) >0
go
