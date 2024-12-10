--1.
WITH ranked_rows AS (
    SELECT 
        id, -- 'id' column must be unique or the primary key
        ROW_NUMBER() OVER (
            PARTITION BY game_id, team_id, player_id -- Define duplicate groups
            ORDER BY id -- Retain the row with the lowest id
        ) AS row_num
DELETE FROM game_details
WHERE id IN (
    SELECT id
    FROM ranked_rows
    WHERE row_num > 1)
---2.
drop table user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC ,                      -- Unique identifier for the user
    device_id NUMERIC ,                    -- Unique identifier for the device
    device_activity_datelist JSONB ,       -- Tracks active days by browser_type in a MAP-like format
    PRIMARY KEY (user_id, device_id)               -- Ensures one record per user-device pair
);


--A cumulative query to generate device_activity_datelist from events

-- Step 1: Extract distinct combinations of user_id, device_id, browser_type, and activity_date
WITH distinct_dates AS (
    SELECT 
        user_id,
        device_id,
        browser_type,                           -- Assuming browser_type is present in the events table
        DATE(event_time::timestamp) AS activity_date -- Extract the date part from event_time
    FROM 
        events
    WHERE 
        DATE(event_time::timestamp) BETWEEN '2023-01-01' AND '2023-01-31' -- Filter events within the desired date range
        AND user_id IS NOT NULL
        AND device_id IS NOT NULL
        AND browser_type IS NOT NULL
    GROUP BY 
        user_id, device_id, browser_type, DATE(event_time::timestamp)
),
-- Step 2: Aggregate activity dates into an array per user, device, and browser type
aggregated_activity AS (
    SELECT 
        user_id,
        device_id,
        browser_type,
        ARRAY_AGG(activity_date ORDER BY activity_date) AS activity_dates -- Group dates into an array per browser
    FROM 
        distinct_dates
    GROUP BY 
        user_id, device_id, browser_type
),
-- Step 3: Build the JSONB object for each user and device, mapping browser_type to its array of activity dates
jsonb_activity AS (
    SELECT 
        user_id,
        device_id,
        jsonb_object_agg(browser_type, activity_dates::JSONB) AS device_activity_datelist -- Build JSONB object
    FROM 
        aggregated_activity
    GROUP BY 
        user_id, device_id
)
-- Step 4: Insert the aggregated JSONB object into the user_devices_cumulated table
INSERT INTO user_devices_cumulated (user_id, device_id, device_activity_datelist)
SELECT 
    user_id,
    device_id,
    device_activity_datelist -- The JSONB object containing browser_type and corresponding activity dates
FROM 
    jsonb_activity
ON CONFLICT (user_id, device_id) DO NOTHING; -- Avoiding inserting duplicates for the same user_id and device_id

---3.
-A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

-- Step 1: Add the datelist_int column
ALTER TABLE user_devices_cumulated
ADD COLUMN datelist_int INTEGER[] NOT NULL DEFAULT '{}';

-- Step 2: Populate datelist_int using a combination of POW and bitwise operations
UPDATE user_devices_cumulated
SET datelist_int = ARRAY(
    SELECT 
        CASE 
            -- Handle JSONB arrays
            WHEN jsonb_typeof(device_activity_datelist::jsonb) = 'array' THEN
                (SELECT ARRAY_AGG(
                    CAST(
                        (EXTRACT(YEAR FROM value::DATE)::INTEGER * POW(10, 4)::INTEGER) + -- YEAR * 10^4 (shift by 4 decimal places)
                        (EXTRACT(MONTH FROM value::DATE)::INTEGER * POW(10, 2)::INTEGER) + -- MONTH * 10^2 (shift by 2 decimal places)
                        EXTRACT(DAY FROM value::DATE)::INTEGER                           -- Add DAY (units place)
                    AS INTEGER)
                )
                FROM jsonb_array_elements_text(device_activity_datelist::jsonb))
            -- Handle standard ARRAYs
            ELSE
                ARRAY(
                    SELECT 
                        CAST(
                            (EXTRACT(YEAR FROM d)::INTEGER * POW(10, 4)::INTEGER) +  -- YEAR * 10^4
                            (EXTRACT(MONTH FROM d)::INTEGER * POW(10, 2)::INTEGER) + -- MONTH * 10^2
                            EXTRACT(DAY FROM d)::INTEGER                            -- DAY (units place)
                        AS INTEGER)
                    FROM unnest(device_activity_datelist) AS d
                )
        END
);

---4.
CREATE TABLE hosts_cumulated (
    host TEXT ,                  
    host_activity_datelist DATE[] , 
    PRIMARY KEY (host)                  
);
--The incremental query to generate host_activity_datelist

-- Step 1: Extract distinct host and activity_date combinations
WITH distinct_host_dates AS (
    SELECT 
        host,
        DATE(event_time::timestamp) AS activity_date -- Extract the date part from event_time
    FROM 
        events
    WHERE 
        event_time IS NOT NULL AND host IS NOT NULL -- Ensure valid data for event_time and host
    GROUP BY 
        host, DATE(event_time::timestamp) -- Group by host and date to remove duplicates
),

-- Step 2: Aggregate new activity dates into arrays per host
aggregated_new_activity AS (
    SELECT 
        host,
        ARRAY_AGG(DISTINCT activity_date ORDER BY activity_date) AS new_activity_datelist 
        -- ARRAY_AGG: Aggregates unique (DISTINCT) activity dates into an array.
        -- ORDER BY: Ensures the dates in the array are sorted chronologically.
    FROM 
        distinct_host_dates
    GROUP BY 
        host -- Grouping ensures one array per host
),

-- Step 3: Merge existing host_activity_datelist with new activity
merged_activity AS (
    SELECT 
        a.host,
        ARRAY(
            SELECT DISTINCT unnest(
                COALESCE(h.host_activity_datelist, '{}') || a.new_activity_datelist
            )
        ) AS updated_host_activity_datelist
        -- COALESCE: If the host already exists but has no previous activity (NULL), initialize with an empty array '{}'.
        -- || (Array Concatenation): Combines the existing activity dates with the new ones.
        -- DISTINCT unnest(...): Deduplicates the merged array, ensuring no duplicate dates are present.
    FROM 
        aggregated_new_activity a
    LEFT JOIN 
        hosts_cumulated h
    ON 
        a.host = h.host -- Join to find existing activity data for the same host
)

-- Step 4: Insert new hosts or update existing ones
INSERT INTO hosts_cumulated (host, host_activity_datelist)
SELECT 
    merged_activity.host,
    merged_activity.updated_host_activity_datelist
FROM 
    merged_activity
ON CONFLICT (host) DO UPDATE
SET 
    host_activity_datelist = EXCLUDED.host_activity_datelist; 
    -- ON CONFLICT: Ensures no duplicate rows are inserted for the same host.
    -- DO UPDATE: Updates the host_activity_datelist with the merged array.
	

---5.
CREATE TABLE host_activity_reduced (
    month DATE NOT NULL,                       -- Represents the month (e.g., '2023-01-01' for January 2023)
    host TEXT NOT NULL,                        -- The host identifier (e.g., domain name)
    hit_array BIGINT[] NOT NULL,               -- Stores daily hit counts as an array (COUNT(1) per day)
    unique_visitors_array BIGINT[] NOT NULL,   -- Stores daily unique visitor counts as an array (COUNT(DISTINCT user_id) per day)
    PRIMARY KEY (month, host)                  -- Ensures each host has a unique record for each month
);
--An incremental query that loads host_activity_reduced day-by-day

-- Step 1: Aggregate daily data from the `events` table
WITH daily_aggregates AS (
    SELECT
        DATE_TRUNC('month', DATE(event_time)) AS month, -- Extract the month of the event
        host,                                          -- Host identifier
        EXTRACT(DAY FROM DATE(event_time))::INT AS day, -- Extract the day of the month
        COUNT(1) AS daily_hits,                        -- Total daily hits (COUNT(1))
        COUNT(DISTINCT user_id) AS daily_unique_visitors -- Unique daily visitors (COUNT DISTINCT user_id)
    FROM
        events
    WHERE
        event_time IS NOT NULL                          -- Ensure valid event_time
        AND host IS NOT NULL                            -- Ensure valid host
        AND user_id IS NOT NULL                         -- Ensure valid user_id
    GROUP BY
        DATE_TRUNC('month', DATE(event_time)),          -- Group by month
        host,                                           -- Group by host
        EXTRACT(DAY FROM DATE(event_time))              -- Group by day
),

merged_monthly_data AS (
    SELECT
        d.month,
        d.host,
        -- Generate the updated hit_array for the month
        ARRAY(
            SELECT 
                CASE 
                    -- Handle valid days for the current month
                    WHEN i <= EXTRACT(DAY FROM DATE_TRUNC('month', d.month) + INTERVAL '1 month' - INTERVAL '1 day') THEN
                        COALESCE(h.hit_array[i], 0) + CASE WHEN i = d.day THEN d.daily_hits ELSE 0 END
                    -- Ignore days beyond the end of the month
                    ELSE NULL
                END
            FROM generate_series(1, 31) AS i -- Generate an array for all possible days (up to 31)
        ) FILTER (WHERE i IS NOT NULL) AS hit_array, -- Filter out invalid days
        -- Generate the updated unique_visitors_array for the month
        ARRAY(
            SELECT 
                CASE 
                    WHEN i <= EXTRACT(DAY FROM DATE_TRUNC('month', d.month) + INTERVAL '1 month' - INTERVAL '1 day') THEN
                        COALESCE(h.unique_visitors_array[i], 0) + CASE WHEN i = d.day THEN d.daily_unique_visitors ELSE 0 END
                    ELSE NULL
                END
            FROM generate_series(1, 31) AS i
        ) FILTER (WHERE i IS NOT NULL) AS unique_visitors_array
    FROM
        daily_aggregates d
    LEFT JOIN
        host_activity_reduced h
    ON
        d.month = h.month AND d.host = h.host -- Match the existing monthly data for the same host
)

-- Step 3: Insert or update the aggregated data into the reduced fact table
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT
    m.month,
    m.host,
    m.hit_array,
    m.unique_visitors_array
FROM
    merged_monthly_data m
ON CONFLICT (month, host) 
DO UPDATE
SET 
    hit_array = EXCLUDED.hit_array,                     -- Update hit_array with new data
    unique_visitors_array = EXCLUDED.unique_visitors_array; -- Update unique_visitors_array with new data

















