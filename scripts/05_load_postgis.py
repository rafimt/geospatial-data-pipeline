"""
Step 05: Load processed vector and raster data into PostGIS.

Uses:
- ogr2ogr (via subprocess) for vector loading
- raster2pgsql + psql (via subprocess) for raster loading
- psycopg2 for schema setup and verification

Prerequisites:
    - Docker container running: docker-compose up -d
    - GDAL tools available: ogr2ogr, raster2pgsql, psql

Usage:
    conda activate geospatial
    python scripts/05_load_postgis.py
"""

import os
import subprocess
import psycopg2

# raster2pgsql and psql are part of PostgreSQL, not on PATH by default on Windows
PG_BIN = r"C:\Program Files\PostgreSQL\17\bin"
RASTER2PGSQL = os.path.join(PG_BIN, "raster2pgsql.exe")
PSQL = os.path.join(PG_BIN, "psql.exe")

# --- Database connection settings ---
# These values match what is set in docker-compose.yml for the PostGIS container
DB = {
    "host": "127.0.0.1",    # the database is running on this machine (inside Docker)
    "port": 5433,           # mapped host port (5433→5432 in Docker; avoids conflict with local postgres)
    "dbname": "geospatial", # the name of the database we created
    "user": "geouser",      # the database username
    "password": "geopass",  # the database password
}

# DSN = Data Source Name — a plain-text connection string used by command-line tools
# ogr2ogr expects the format: host=... dbname=... user=... password=...
PG_DSN = f"host={DB['host']} port={DB['port']} dbname={DB['dbname']} user={DB['user']} password={DB['password']}"

# ogr2ogr needs the DSN wrapped in PG:"..." to know it's a PostgreSQL target
PG_OGR = f"PG:{PG_DSN}"

PROC_DIR = r'C:\RMTPROJECTS\dataengineering\geospatial\data\processed'  # folder where our processed files live
RAW_DIR = r'C:\RMTPROJECTS\dataengineering\geospatial\data\raw'          # folder where raw source files live (not used directly here)
SRID = 32613                  # coordinate system: UTM Zone 13N (EPSG code)


def get_connection():
    """Open and return a new connection to the PostGIS database."""
    return psycopg2.connect(**DB)  # **DB unpacks the dictionary as keyword arguments: host=..., port=..., etc.


def run_sql(sql: str, description: str = ""):
    """
    Run one or more SQL statements against the PostGIS database.
    Opens a connection, executes the SQL, commits, then closes.
    """
    with get_connection() as conn:       # 'with' ensures the connection is closed when done
        with conn.cursor() as cur:       # cursor = the object you use to send SQL to the database
            cur.execute(sql)             # send the SQL string to PostgreSQL to be executed
        conn.commit()                    # commit = save the changes permanently (like hitting Save)
    if description:                      # only print if a description was provided
        print(f"  OK: {description}")    # confirm success to the user


def ogr2ogr_load(src_path: str, table: str, geom_type: str = "PROMOTE_TO_MULTI"):
    """
    Load a vector file (GeoPackage, Shapefile, etc.) into a PostGIS table.
    Uses the ogr2ogr command-line tool from GDAL — Python calls it as a subprocess.
    """
    schema, tbl = table.split(".")      # split "raw.buildings" into schema="raw", tbl="buildings"
    cmd = [
        "ogr2ogr",                      # the GDAL tool that reads/writes spatial formats
        "-f", "PostgreSQL",             # output format: write to a PostgreSQL database
        PG_OGR,                         # connection string telling ogr2ogr where the database is
        src_path,                       # input file path (e.g. data/processed/buildings_utm.gpkg)
        "-nln", table,                  # nln = new layer name — the destination table (e.g. raw.buildings)
        "-nlt", geom_type,              # nlt = new layer type — PROMOTE_TO_MULTI allows mixed geom types
        "-lco", "GEOMETRY_NAME=geom",   # lco = layer creation option — name the geometry column "geom"
        "-lco", "FID=id",               # name the primary key column "id"
        "-lco", f"SCHEMA={schema}",     # write to this schema (raw, processed, etc.)
        "-overwrite",                   # drop and recreate the table if it already exists
        "--config", "PG_USE_COPY", "YES",  # use PostgreSQL COPY instead of INSERT — much faster for bulk loads
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)  # run ogr2ogr and wait for it to finish
    # capture_output=True means stdout/stderr are captured into result instead of printed
    # text=True means the output is returned as a string instead of raw bytes
    if result.returncode != 0:                          # returncode 0 = success, anything else = error
        print(f"  ERROR loading {src_path}: {result.stderr}")  # print the error message from ogr2ogr
        return False                                    # signal that loading failed
    print(f"  Loaded: {src_path} → {table}")           # confirm success
    return True                                         # signal that loading succeeded


def raster2pgsql_load(tif_path: str, table: str, srid: int = SRID, tile_size: str = "256x256"):
    """
    Load a GeoTIFF raster file into a PostGIS raster table.
    Pipes raster2pgsql output directly into psql — equivalent to: raster2pgsql ... | psql ...
    """
    schema, tbl = table.split(".")      # split "raw.dem" into schema="raw", tbl="dem"
    sql_file = f"/tmp/{tbl}_raster.sql" # (unused path — was from an earlier version that wrote to disk)

    # raster2pgsql converts a GeoTIFF into a stream of SQL INSERT statements
    cmd_export = [
        RASTER2PGSQL,           # PostGIS tool that converts rasters to SQL
        f"-s", str(srid),       # set the coordinate system (SRID) for the raster
        "-I",                   # create a spatial index on the raster table
        "-C",                   # apply raster constraints (useful for validation)
        "-M",                   # vacuum analyze after loading (updates query planner statistics)
        "-t", tile_size,        # tile the raster into 256x256 pixel chunks for faster queries
        tif_path,               # input GeoTIFF file
        table,                  # destination PostGIS table (e.g. raw.dem)
    ]

    # psql reads the SQL from raster2pgsql and executes it against the database
    cmd_import = [PSQL, "-h", DB["host"], "-p", str(DB["port"]), "-U", DB["user"], "-d", DB["dbname"]]

    print(f"  Loading raster: {tif_path} → {table} (tiling {tile_size})...")

    # Popen launches a process without waiting — unlike subprocess.run, it doesn't block
    # stdout=subprocess.PIPE means we capture the output so we can pipe it to the next process
    r2p = subprocess.Popen(
        cmd_export,
        stdout=subprocess.PIPE,   # capture raster2pgsql's SQL output
        stderr=subprocess.PIPE,   # capture any error messages
        env={**os.environ, "PGPASSWORD": DB["password"]}  # pass the password via environment variable
    )

    # psql reads from stdin=r2p.stdout — this connects raster2pgsql's output directly into psql's input
    # This is the Python equivalent of the shell pipe: raster2pgsql ... | psql ...
    psql = subprocess.Popen(
        cmd_import,
        stdin=r2p.stdout,         # psql reads the SQL that raster2pgsql is writing
        stdout=subprocess.PIPE,   # capture psql's output
        stderr=subprocess.PIPE,   # capture psql's error messages
        env={**os.environ, "PGPASSWORD": DB["password"]}  # pass the password via environment variable
    )

    r2p.stdout.close()            # close our end of the pipe — psql now owns it; this prevents a deadlock
    stdout, stderr = psql.communicate()  # wait for psql to finish and collect its output

    if psql.returncode != 0:                        # non-zero = something went wrong
        print(f"  ERROR: {stderr.decode()[:300]}")  # decode bytes to string, show first 300 chars of error
        return False
    print(f"  Loaded raster → {table}")
    return True


def setup_schemas():
    """Create the PostGIS extensions and database schemas if they don't already exist."""
    print("Setting up schemas and extensions...")
    run_sql("""
        CREATE EXTENSION IF NOT EXISTS postgis;         -- adds core PostGIS spatial types and functions
        CREATE EXTENSION IF NOT EXISTS postgis_raster;  -- adds raster support (ST_Raster, raster_columns, etc.)
        CREATE SCHEMA IF NOT EXISTS raw;                -- schema for data loaded directly from source files
        CREATE SCHEMA IF NOT EXISTS processed;          -- schema for cleaned/transformed data
        CREATE SCHEMA IF NOT EXISTS analysis;           -- schema for query results and derived datasets
    """, "schemas and extensions ready")


def create_processed_buildings():
    """
    Create processed.buildings as a clean copy of raw.buildings.
    Parses the height string, estimates floors, computes area, and fixes invalid geometries.
    """
    print("Creating processed.buildings view...")
    run_sql("""
        DROP TABLE IF EXISTS processed.buildings;   -- remove the table if it already exists so we can recreate it

        CREATE TABLE processed.buildings AS         -- CREATE TABLE AS runs a SELECT and saves the results as a new table
        SELECT
            id,                                     -- copy the primary key from raw.buildings
            name,                                   -- copy the building name (may be NULL if OSM has no name)
            building,                               -- copy the building type tag from OSM (e.g. 'yes', 'residential')
            -- height in OSM is stored as text like "12.5" or "3 floors" — we only keep numeric values
            CASE WHEN height ~ '^[0-9.]+$'          -- ~ is a regex match; this checks if height is a plain number
                THEN height::numeric                -- ::numeric casts the text to a number
                ELSE NULL                           -- if it's not a clean number, store NULL
            END AS height_m,
            -- estimate floors by dividing height by 3 (approx. 3 metres per floor)
            CASE WHEN height ~ '^[0-9.]+$'
                THEN ROUND(height::numeric / 3)     -- ROUND() rounds to nearest integer
                ELSE NULL
            END AS est_floors,
            ROUND(ST_Area(geom)::numeric, 1) AS area_m2,  -- ST_Area returns the polygon area in CRS units (metres²)
            ST_SetSRID(ST_MakeValid(geom), 32613) AS geom  -- repair geometry and explicitly set SRID so geometry_columns shows 32613
        FROM raw.buildings
        -- only include rows where the geometry is already valid OR can be made valid
        WHERE ST_IsValid(geom) OR ST_IsValid(ST_MakeValid(geom));

        -- GIST index = spatial index; makes geometry queries (intersects, contains, etc.) fast
        CREATE INDEX buildings_proc_geom_idx ON processed.buildings USING GIST(geom);
    """, "processed.buildings created with spatial index")


def verify_loads():
    """Query each loaded table and print row counts and SRIDs to confirm everything loaded correctly."""
    print("\nVerification:")
    print(f"  {'Layer':<35} {'Count':>10}  {'SRID'}")  # header row, padded for alignment
    print("  " + "-" * 55)                              # separator line

    # list of vector tables to check — (schema, table_name)
    vector_tables = [
        ("raw", "buildings"),
        ("raw", "roads"),
        ("raw", "landuse"),
        ("processed", "buildings"),
    ]
    # list of raster tables to check
    raster_tables = [
        ("raw", "dem"),
        ("raw", "ndsm"),
        ("raw", "nlcd"),
    ]

    with get_connection() as conn:       # open one connection and reuse it for all checks
        with conn.cursor() as cur:       # one cursor handles all queries in this block
            for schema, table in vector_tables:   # loop through each vector table
                try:
                    # ST_SRID returns the coordinate system ID stored on the geometry column
                    # GROUP BY 2 groups by the second column (SRID) — assumes all rows have the same SRID
                    cur.execute(f"SELECT COUNT(*), ST_SRID(geom) FROM {schema}.{table} GROUP BY 2 LIMIT 1")
                    row = cur.fetchone()           # fetchone returns the first (and only) result row, or None
                    if row:
                        print(f"  {schema}.{table:<33} {row[0]:>10,}  {row[1]}")  # row[0]=count, row[1]=srid
                    else:
                        print(f"  {schema}.{table:<33} {'0':>10}")  # table exists but is empty
                except Exception as e:
                    conn.rollback()                                  # clear the aborted transaction so next query works
                    print(f"  {schema}.{table:<33} ERROR: {e}")     # table may not exist yet

            for schema, table in raster_tables:   # loop through each raster table
                try:
                    # raster_columns is a PostGIS system view that tracks metadata about raster tables
                    # %s placeholders are safely substituted by psycopg2 to prevent SQL injection
                    cur.execute(
                        f"SELECT srid FROM raster_columns WHERE r_table_schema=%s AND r_table_name=%s",
                        (schema, table)            # these values are safely substituted for the %s placeholders
                    )
                    row = cur.fetchone()           # get the metadata row (or None if not registered)
                    if row:
                        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")  # count the actual raster tiles
                        tiles = cur.fetchone()[0]  # [0] gets the count value from the single-column result
                        print(f"  {schema}.{table:<33} {tiles:>10,} tiles  SRID={row[0]}")
                    else:
                        print(f"  {schema}.{table:<33} NOT LOADED")  # not in raster_columns = not loaded
                except Exception as e:
                    conn.rollback()                                  # clear the aborted transaction so next query works
                    print(f"  {schema}.{table:<33} ERROR: {e}")


def main():
    """Run the full Step 05 pipeline: setup → vector loads → processed tables → raster loads → verify."""
    print("=" * 60)
    print("Geospatial Pipeline — Step 05: PostGIS Integration")
    print("=" * 60)

    setup_schemas()   # ensure extensions and schemas exist before loading anything

    # Each tuple is: (source file path, destination table, geometry type)
    vector_loads = [
        (f"{PROC_DIR}/buildings_utm.gpkg", "raw.buildings", "PROMOTE_TO_MULTI"),
        (f"{PROC_DIR}/roads_utm.gpkg", "raw.roads", "PROMOTE_TO_MULTI"),
        (f"{PROC_DIR}/landuse_utm.gpkg", "raw.landuse", "PROMOTE_TO_MULTI"),
        (f"{PROC_DIR}/roads_buffered.gpkg", "processed.road_buffers", "PROMOTE_TO_MULTI"),
    ]

    print("\nLoading vector layers...")
    for src, table, geom in vector_loads:       # unpack each tuple into src, table, geom
        if os.path.exists(src):                 # only try to load if the file actually exists
            ogr2ogr_load(src, table, geom)
        else:
            print(f"  SKIP (not found): {src}") # warn but don't crash if a file is missing

    create_processed_buildings()   # build the cleaned buildings table from what was just loaded

    # Each tuple is: (source GeoTIFF path, destination raster table)
    raster_loads = [
        (f"{PROC_DIR}/dem_filled.tif", "raw.dem"),
        (f"{PROC_DIR}/ndsm.tif", "raw.ndsm"),
        (f"{PROC_DIR}/nlcd_utm.tif", "raw.nlcd"),
        (f"{PROC_DIR}/slope.tif", "raw.slope"),
    ]

    print("\nLoading raster layers...")
    for tif, table in raster_loads:             # unpack each tuple into tif path and table name
        if os.path.exists(tif):                 # skip missing files gracefully
            raster2pgsql_load(tif, table)
        else:
            print(f"  SKIP (not found): {tif}")

    verify_loads()   # print a summary table confirming row counts and SRIDs
    print("\nStep 05 complete. Proceed to Step 06 (Spatial Analysis).")


if __name__ == "__main__":
    main()   # only run main() if this script is executed directly (not imported as a module)
