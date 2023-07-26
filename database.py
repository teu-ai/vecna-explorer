import pandas as pd
import os
import json
import logging
import duckdb

logging.basicConfig(level=logging.INFO)

def load_itinerarios_json_to_dataframe(json_dir="data", months=["June","July"]):
    """ Load itineraries from JSON files in directory, and returns DataFrame

    The function expectes individual JSON files returned from Project44 API in the specified directory.

    Args:
        json_dir (str): Directory where JSON files are located
        months (list): List of months to filter itineraries
    """
    data = []
    for month in months:
        # Read all json files from data directory
        data_dir = f"{json_dir}/{month}/"
        files = [f for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f)) and f.endswith(".json")]
        for file in files:
            # Read JSON
            with open(data_dir+file) as f:
                d = json.load(f)
                if not isinstance(d, dict):
                    logging.warning(f"JSON file has unrecognized format; parent is not a dictionary: {file}")
                    continue
                if "results" not in d.keys():
                    logging.warning(f"JSON file has unrecognized format; no 'results' key found: {file}")
                    continue
                data += [d]
    results = [i for sublist in [d["results"] for d in data] for i in sublist]
    return pd.DataFrame(results)

def load_itinerarios_dataframe_to_db(df, database="itineraries.db"):
    con = duckdb.connect(database)
    con.execute("CREATE OR REPLACE TABLE test AS SELECT * FROM df")
    con.execute("INSERT INTO test SELECT * FROM df")
    con.close()

def get_itinerarios(database="itineraries.db"):
    con = duckdb.connect(database)
    df = con.execute("SELECT * FROM test").fetchdf()
    con.close()
    return df

def main():
    database = "itineraries.db"
    df = load_itinerarios_json_to_dataframe()
    load_itinerarios_dataframe_to_db(df, database="itineraries.db")
    logging.info(f"Loaded {(len(get_itinerarios(database=database)))} itineraries to {database}")

if __name__ == '__main__':
    main()