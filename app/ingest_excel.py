"""
Ingest an Excel file into MongoDB dynamically.

Usage:
  python app/ingest_excel.py --file data/shipment_data.xlsx --mongo-uri mongodb://localhost:27017 --db shipments_db --collection shipments

If sheet_name is not provided, the first sheet is used.
"""
import argparse
import os
import sys
import math
import pandas as pd
from pymongo import MongoClient
from dateutil import parser as dateparser

# Fields we should treat as strings even if numeric-looking
ID_FIELDS_HINTS = ["ref", "tracking", "po", "so", "awb", "id"]

MAX_INT64 = 9223372036854775807

def try_parse_number(val):
    try:
        if pd.isna(val):
            return None
        if isinstance(val, (int, float)) and not (isinstance(val, float) and math.isnan(val)):
            if isinstance(val, int) and abs(val) > MAX_INT64:
                return str(val)
            return val
        s = str(val).strip()
        if s == "":
            return None
        s2 = s.replace(",", "")
        # allow decimals and negative
        if s2.replace(".", "", 1).lstrip("+-").isdigit():
            num = float(s2) if "." in s2 else int(s2)
            if isinstance(num, int) and abs(num) > MAX_INT64:
                return s
            return num
    except Exception:
        pass
    return val

def try_parse_date(val):
    try:
        if pd.isna(val):
            return None
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime()
        s = str(val).strip()
        if s == "":
            return None
        # dateutil parser - flexible
        return dateparser.parse(s, fuzzy=True)
    except Exception:
        return val

def is_id_field_name(name):
    n = name.lower()
    return any(h in n for h in ID_FIELDS_HINTS)

def normalize_row(row):
    obj = {}
    for k, v in row.items():
        if is_id_field_name(str(k)):
            obj[k] = None if pd.isna(v) else str(v)
            continue
        num = try_parse_number(v)
        # prefer numeric if parsed numeric (and not a string)
        if num is not None and not isinstance(num, str):
            obj[k] = num
            continue
        dt = try_parse_date(v)
        if dt is not None and not isinstance(dt, str):
            obj[k] = dt
            continue
        obj[k] = None if pd.isna(v) else v
    return obj

def ingest(file_path, mongo_uri, db_name, coll_name, sheet_name=None):
    if not os.path.exists(file_path):
        print("File not found:", file_path)
        sys.exit(1)

    # read excel; if multiple sheets and sheet_name None, pick first
    if sheet_name:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=object)
    else:
        tmp = pd.read_excel(file_path, sheet_name=None, dtype=object)
        if isinstance(tmp, dict):
            sheet_keys = list(tmp.keys())
            if not sheet_keys:
                print("ERROR: no sheets found")
                sys.exit(1)
            first = sheet_keys[0]
            print("No --sheet-name provided. Using first sheet:", first)
            df = tmp[first]
        else:
            df = tmp

    if not isinstance(df, pd.DataFrame):
        print("ERROR: failed to read dataframe from excel")
        sys.exit(1)

    cols = list(df.columns)
    print("Columns detected:", cols)
    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    docs = [normalize_row(row) for _, row in df.iterrows()]
    if docs:
        res = coll.insert_many(docs)
        print(f"Inserted {len(res.inserted_ids)} documents into {db_name}.{coll_name}")
    else:
        print("No rows to insert")

    # create simple indexes for common fields (best-effort)
    hints = ("ship", "date", "status", "cost", "charge", "ref", "tracking", "origin", "destination", "to", "from")
    for c in cols:
        lc = c.lower()
        if any(h in lc for h in hints):
            try:
                coll.create_index([(c, 1)])
                print("Created index on", c)
            except Exception as e:
                print("Index create failed for", c, ":", e)
    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--db", default="shipments_db")
    parser.add_argument("--collection", default="shipments")
    parser.add_argument("--sheet-name", default=None)
    args = parser.parse_args()
    ingest(args.file, args.mongo_uri, args.db, args.collection, args.sheet_name)

