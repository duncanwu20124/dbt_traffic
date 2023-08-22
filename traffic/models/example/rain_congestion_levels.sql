-- models/rain_congestion_levels.sql
SELECT
    locationid,
    congestionlevelid,
    parametername
FROM {{ ref('weather_conditions') }}
WHERE congestionlevelid IS NOT NULL
