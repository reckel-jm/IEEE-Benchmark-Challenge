import csv
from influxdb import InfluxDBClient
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

import time

# --- The configuration is loaded from the .env file
HOST = os.getenv('INFLUX_HOST')
PORT = os.getenv('INFLUX_HOST_PORT', 8086)
USERNAME = os.getenv('INFLUX_USERNAME', '')
PASSWORD = os.getenv('INFLUX_PASSWORD', '')
DATABASE = os.getenv('INFLUX_DATABASE', '')

# Time range and chunk size for querying
TOTAL_START_MS = 1735686000000  # 01.01.2025
TOTAL_END_MS   = 1767221999999  # 31.12.2025
CHUNK_SIZE_MS  = 14 * 24 * 60 * 60 * 1000  # 14 days

# The running query template with placeholders for start and end timestamps
# Parameters:
# "name": A name for the query, used for logging.
# "query_template": The InfluxDB query template with placeholders {start} and {end} for the time range in milliseconds.
# "filename": The name of the CSV file where the results will be saved.
queries =  [ {
    "name": "weather",
    "query_template": """
SELECT mean("EnvTmp") as "temperature [°C]",
  mean("EnvHum") as "humidity [%]",
  mean("EnvPres") as "pressure [hPa]",
  mean("DiffInsol") as "directed_solar_radiation [W/m²]",
  mean("EnvBgtns") as "environment_brightness [lux]",
  mean("BgtnsDir") as "brightness_direction [°]",
  mean("SnAzmth") as "solar_azimuth [°]",
  mean("SnIncln") as "solar_inclination [°]",
  mean("RnFll") as "rainfall [mm/d]",
  mean("RnInts") as "rainfall_intensity [mm/h]",
  mean("HorWdDir") as "horizontal_wind_direction [°]",
  mean("HorWdSpd") as "horizontal_wind_speed [m/s]"
  FROM "weather" 
  WHERE "device"::tag =~ /668$/ AND time >= {start}ms and time <= {end}ms 
  GROUP BY time(1m)
  ORDER BY time ASC
""",
    "filename": "weather_data.csv"
},
{
    "name": "power",
    "query_template": """
SELECT mean("uL1N") / 10 AS "u_L1", 
    mean("uL2N") / 10 AS "u_L2", 
    mean("uL3N") / 10 AS "u_L3", 
    mean("iL1") / 1000 AS "i_L1", 
    mean("iL2") / 1000 AS "i_L2", 
    mean("iL3") / 1000 AS "i_L3", 
    mean("pL1") AS "p_L1", 
    mean("pL2") AS "p_L2", 
    mean("pL3") AS "p_L3", 
    mean("sL1") AS "s_L1", 
    mean("sL2") AS "s_L2", 
    mean("sL3") AS "s_L3", 
    mean("qL1") AS "q_L1", 
    mean("qL2") AS "q_L2", 
    mean("qL3") AS "q_L3" 
    FROM /^WP\\.J\\.8$/ 
    WHERE time >= {start}ms AND time < {end}ms 
    GROUP BY time(1m);
""",
    "filename": "power_data.csv"
}
]



def export_influx_to_csv():
    # Initialize InfluxDB client
    client = InfluxDBClient(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, ssl=True, verify_ssl=True)
    client.switch_database(DATABASE)

    for query_info in queries:
        query_name: str = query_info["name"]
        query_template: str = query_info["query_template"]
        filename: str = query_info["filename"]
        print(f"\nStarting export for query: {query_name}...")


        current_start = TOTAL_START_MS
        is_first_chunk = True

        while current_start < TOTAL_END_MS:
            current_end = min(current_start + CHUNK_SIZE_MS, TOTAL_END_MS)
            
            # Fill the query template with the current start and end timestamps
            query = query_template.format(start=current_start, end=current_end)
            
            print(f"Query running for: {current_start} till {current_end}...")
            
            try:
                result = client.query(query)
                points = list(result.get_points())

                if points:
                    headers = points[0].keys()
                    # WHen writing the first chunk, we write the header. For subsequent chunks, we append without writing the header again.
                    file_mode = 'w' if is_first_chunk else 'a'
                    
                    with open(filename, mode=file_mode, newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        if is_first_chunk:
                            writer.writeheader()
                            is_first_chunk = False
                        writer.writerows(points)
                
                # Short pause for the server (optional)
                time.sleep(0.1)
                current_start = current_end

            except Exception as e:
                print(f"Error at timestamp {current_start}: {e}")
                break

    print(f"\nDone! All data saved at {filename}.")

if __name__ == "__main__":
    export_influx_to_csv()