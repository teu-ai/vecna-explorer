import pandas as pd
import os
import json
import logging
import duckdb

logging.basicConfig(level=logging.INFO)

def load_itinerarios_json_to_dataframe(data_dir="data"):
    """ Load itineraries from JSON files in directory, and returns DataFrame

    The function expectes individual JSON files returned from Project44 API in the specified directory.

    Args:
        json_dir (str): Directory where JSON files are located
        months (list): List of months to filter itineraries
    """

    data = []

    # Iterate over all sub-diretories in the root directory json_dir
    for subdir, dirs, files in os.walk(data_dir):
        # Read all json files from directory
        files = [f for f in files if f.endswith(".json")]
        for file in files:
            # Read JSON
            with open(f"{subdir}/{file}") as f:
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

def load_itinerarios_dataframe_to_db(df, database="itineraries.db", table="itineraries"):
    """ Load itineraries from DataFrame to database

    Args:
        df (pd.DataFrame): DataFrame with itineraries
        database (str): Database name
        table (str): Table name
    """
    con = duckdb.connect(database)
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df")
    con.execute(f"INSERT INTO {table} SELECT * FROM df")
    con.close()

def get_itinerarios(database="itineraries.db", table="itineraries"):
    """ Get itineraries from database

    Args:
        database (str): Database name
        table (str): Table name
    """
    con = duckdb.connect(database)
    df = con.execute(f"SELECT * FROM {table}").fetchdf()
    con.close()
    return df

def main():
    database = "itineraries.db"
    df = load_itinerarios_json_to_dataframe()
    load_itinerarios_dataframe_to_db(df, database="itineraries.db")
    logging.info(f"Loaded {(len(get_itinerarios(database=database)))} itineraries to {database}")

if __name__ == '__main__':
    main()