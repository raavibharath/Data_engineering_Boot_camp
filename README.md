#Homework
# Dimensional Data Modeling - Week 1

This week's assignment involves working with the `actor_films` dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the actor_films dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the actor_films dataset

## Dataset Overview
The `actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actorid`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `filmid`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Assignment Tasks

1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.
    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
    
2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.
    
3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.
      
4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
    
5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.
# Week 2 Fact Data Modeling
The homework this week will be using the `devices` and `events` dataset

Construct the following eight queries:

- A query to deduplicate `game_details` from Day 1 so there's no duplicates

- A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

- A cumulative query to generate `device_activity_datelist` from `events`

- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

- A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
  
- The incremental query to generate `host_activity_datelist`

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

- An incremental query that loads `host_activity_reduced`
  - day-by-day

Please add these queries into a folder, zip them up and submit [here](https://bootcamp.techcreator.io)
