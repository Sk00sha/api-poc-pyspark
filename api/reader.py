import requests
import json
import json
import datetime
import time
import requests
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, BooleanType, TimestampType
)

# ────────────────────────── JSON + Schema helpers ──────────────────────────

def flatten_json(nested, parent_key="", sep="."):
    items = []
    if isinstance(nested, dict):
        for k, v in nested.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
    elif isinstance(nested, list):
        items.append((parent_key, json.dumps(nested)))
    else:
        items.append((parent_key, nested))
    return dict(items)

def infer_spark_type(value):
    if value is None:
        return StringType()
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType()
    if isinstance(value, float):
        return DoubleType()
    if isinstance(value, str):
        try:
            datetime.datetime.fromisoformat(value)
            return TimestampType()
        except ValueError:
            return StringType()
    return StringType()

# ────────────────────────── Build schema from sample ──────────────────────────
def build_schema(options):
    base_url = options["base_url"].rstrip("/")
    endpoint = options["endpoint"]
    sample_param = options["sample_param"]
    json_path = options.get("json_path", "")
    headers = {"Authorization": options.get("auth_token", "")}

    r = requests.get(f"{base_url}/{endpoint.format(param=sample_param)}", headers=headers)
    r.raise_for_status()
    data = r.json()

    for key in json_path.split("."):
        if key:
            data = data.get(key, {})

    if isinstance(data, list):
        data = data[0]

    flat = flatten_json(data)
    fields = [StructField(k, infer_spark_type(v), True) for k, v in flat.items()]
    return StructType(fields)

# ────────────────────────── Fetch partition (executor side) ──────────────────────────
def fetch_partition(params, schema, options):
    base_url = options["base_url"].rstrip("/")
    endpoint = options["endpoint"]
    json_path = options.get("json_path", "")
    headers = {"Authorization": options.get("auth_token", "")}
    timeout = int(options.get("timeout", 10))
    backoff = float(options.get("backoff", 0.5))
    retries = int(options.get("retries", 3))

    sess = requests.Session()
    sess.headers.update(headers)
    field_names = [f.name for f in schema.fields]

    for param in params:
        url = f"{base_url}/{endpoint.format(param=param)}"
        for attempt in range(1, retries + 1):
            try:
                resp = sess.get(url, timeout=timeout)
                resp.raise_for_status()
                j = resp.json()
                break
            except Exception:
                if attempt == retries:
                    j = None
                else:
                    time.sleep(backoff * (2 ** (attempt - 1)))
        if j is None:
            continue

        # traverse JSON path
        for key in json_path.split("."):
            if key:
                j = j.get(key, {})

        items = j if isinstance(j, list) else [j]
        for elem in items:
            flat = flatten_json(elem)
            yield tuple(flat.get(f, None) for f in field_names)

def fetch_partition_from_ids(ids, schema, options):
    base_url = options["base_url"].rstrip("/")
    endpoint = options["endpoint"]
    auth_token = options.get("auth_token")

    headers = {"Authorization": auth_token} if auth_token else {}
    session = requests.Session()
    session.headers.update(headers)

    field_names = [f.name for f in schema.fields]

    for id_val in ids:
        url = f"{base_url}/{endpoint.format(param=id_val)}"
        try:
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                data = [data]
            for row in data:
                yield tuple(str(row.get(f, None)) for f in field_names)
        except:
            # yield null row on error
            yield tuple([None]*len(field_names))
