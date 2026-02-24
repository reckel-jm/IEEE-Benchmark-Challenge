# Script for Querying Data from LLEC Influx DB

This script has been used to query data form the LLEC Influx DB at Karlsruhe Institute of Technology (KIT). 

## Steps for Running the Script

1. Create a virtual environment in python and activate it (under Linux, run `python -m venv .venv && source .venv/bin/activate`)
2. Install all requirements with `pip install -r requirements.txt`
3. Add a file .env with the connection parameters:

```dotenv
INFLUX_HOST_PORT=8086
INFLUX_HOST=''
INFLUX_USERNAME=''
INFLUX_PASSWORD = ''
INFLUX_DATABASE = ''
```

4. Adjust the `queries` array in the `main.py` if necessary.
5. Run the program with `python main.py`.

After the successful execution of the script, the generated output files (*.csv) have been saved to the same folder.