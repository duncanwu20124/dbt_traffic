WITH rain_conditions AS (
    SELECT
        wd.locationid,
        wd.parametername,
        dgh.congestionlevelid
    FROM {{ source('traffic_source','directorate_of_general_highways') }} AS dgh
    JOIN {{ source('weather_source','weather_data') }} AS wd ON dgh.sectionid = wd.locationid
    JOIN {{ source('traffic_source','taipei_traffic') }} AS tt ON dgh.sectionid = tt.sectionid
    JOIN {{ source('traffic_source','freeway_bureau') }} AS fb ON dgh.sectionid = fb.sectionid
    WHERE wd.parametername = '多雲時陰陣雨或雷雨' OR wd.parametername = '陰時多雲陣雨或雷雨'
    --WHERE wd.parametername = '舒適至悶熱' OR wd.parametername = '悶熱至易中暑' OR wd.parametername = '多雲' OR wd.parametername = '舒適至悶熱'
)
SELECT distinct
    locationid,
    congestionlevelid,
    parametername
FROM rain_conditions
