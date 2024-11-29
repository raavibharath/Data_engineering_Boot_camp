1.
DROP TYPE films CASCADE;
CREATE TYPE films AS (
    film TEXT,     
    votes INT,         
    rating NUMERIC(3,1),
    filmid text)
------
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
ALTER TYPE quality_class ADD VALUE 'avg';
------					
DROP TABLE IF EXISTS actors;
create table actors(
             actorid text,
			 actor text,
			 current_year int,
			 films films[],
			 quality_class quality_class,
			 is_active boolean,
  PRIMARY KEY (actorid, current_year)  ),
-----------------
--2.
insert into actors
with last_year as (
    select * from actors 
    where current_year = 2020
), this_year as (
    select * from actor_films 
    where year = 2021
), films_and_ratings as (
    select actorid, actor, year,
	Array_agg(row(ty.film, ty.votes, ty.rating, ty.filmid)::films) as current_films,
	avg(rating) as avg_rating
    from this_year as ty
    group by actorid, actor,year
)
    select 
    coalesce(ly.actorid, ty.actorid) As actorid,
    coalesce(ly.actor, ty.actor) as actor,
    coalesce(ly.current_year + 1, ty.year) as current_year,
    case 
       when ly.current_year is null then ty.current_films
	   when ty.year is null then ly.films
	   else ly.films ||ty.current_films
    end::films[] as films,
    case 
	    when ty.avg_rating is null then ly.quality_class
	else
	case
        when ty.avg_rating > 8 then 'star'
        when ty.avg_rating > 7 then 'good'
        when ty.avg_rating > 6 then 'average'  
        when ty.avg_rating <= 6 then 'bad'
        end::quality_class 
	end::quality_class,
    case 
        when ty.actorid is null then false
        else true
    end as is_active
	from films_and_ratings as ty
	full outer join last_year as ly on ly.actorid = ty.actorid
	
---------------------------------------------------------------------------------------------
--3.
create table actors_history_scd(
	actorid text,
	is_active boolean,
	quality_class quality_class,
	current_year integer,
	start_date integer,
	end_date integer
) 

--4.
delete from actors_history_scd
insert into actors_history_scd
with previous as (
	   select actorid, current_year, quality_class,is_active,
       lag(quality_class,1) over(partition by actorid order by current_year) as pre_qlty_year,
	   lag(is_active,1) over(partition by actorid order by current_year) as pre_is_active
	   from actors
	   where current_year <= 2021
),
with_indicators	  as( 
             select *,
	   		 case when quality_class <> pre_qlty_year then 1 
			      when is_active <> pre_is_active then 1 
				  else 0
			end as change_indicator
			from previous
),
with_streaks as (
				select *,
				sum(change_indicator) over (partition by actorid order by current_year) as streak_indifi
				from with_indicators
)
select actorid,is_active, quality_class, 2020 as current_year,
       min(current_year) as start_date,
	   max(current_year) as end_date
from with_streaks
group by actorid,streak_indifi,is_active, quality_class
order by actorid,start_date


--5.
create type actors_scd_type as (
			quality_class quality_class,
			is_active boolean,
			start_date integer,
			end_date integer
)

with last_year_scd as (
		select * from actors_history_scd
		where current_year = 2020 and end_date =2020
), with_historical_scd as (
		select actorid, quality_class,is_active, start_date,end_date
		from actors_history_scd
		where current_year= 2020 and end_date < 2020
),this_year as (
		select * from actors 
		where current_year= 2021
),unchganged_records as (

	select 
		 coalesce(ty.actorid, ly.actorid) as actorid,
		 coalesce(ty.quality_class, ly.quality_class) as quality_class,
		 coalesce(ty.is_active, ly.is_active) as is_active,
		 ly.start_date,
		 ty.current_year as end_date
		 from this_year ty
		 join last_year_scd as ly on ty.actorid = ly.actorid and ty.is_active =ly.is_active
),changed_records as (
	select 
		 coalesce(ty.actorid,ly.actorid) as actorid,
		 unnest(array[row(ly.quality_class,ly.is_active,ly.start_date,ly.end_date)::actors_scd_type,
		  			   row(ty.quality_class,ty.is_active,ty.current_year,ty.current_year)::actors_scd_type]) as records
	from this_year ty
	left outer join last_year_scd ly on ly.actorid = ty.actorid
	where (ty.quality_class <> ly.quality_class) or ty.is_active <> ly.is_active
),unnested_chnaged_records as (
	select actorid, 
		(records::actors_scd_type).quality_class,
		(records::actors_scd_type).is_active,
		(records::actors_scd_type).start_date,
		(records::actors_scd_type).end_date
	from changed_records
), new_records as (
	select ty.actorid,ty.quality_class,ty.is_active,ty.current_year as start_date,
			ty.current_year as end_date
	from this_year as ty
	left join last_year_scd as ly on ty.actorid = ly.actorid
	where ly.actorid is null 
)
select * from with_historical_scd
union all 
select * from  unchganged_records
union all 
select * from unnested_chnaged_records
union all 
select * from new_records
order by desc 









	
