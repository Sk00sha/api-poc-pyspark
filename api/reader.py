import requests
import json

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
