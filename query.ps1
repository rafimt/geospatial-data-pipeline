$env:PGPASSWORD = "geopass"
$psql = "C:\Program Files\PostgreSQL\17\bin\psql.exe"

$query = @"

-- put your queries here
SELECT COUNT(*) FROM raw.buildings;

"@

& $psql -h 127.0.0.1 -p 5433 -U geouser -d geospatial -c $query
