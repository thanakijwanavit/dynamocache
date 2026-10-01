"""Example: using Dynamocache from an AWS Lambda function behind DAX.

The Lambda must run inside the same VPC as the DAX cluster (see template.yaml).
The DAX endpoint is read from an environment variable so the same code runs
against direct DynamoDB locally (unset the variable) and DAX in production.

Deploy the accompanying template.yaml with AWS SAM:

    sam build && sam deploy --guided
"""

import os

from dynamocache import Dynamocache

# In production DAX_ENDPOINT is set by the SAM template; locally it's unset and
# the cache falls back to direct DynamoDB mode.
cache = Dynamocache(
    table_name=os.environ.get("CACHE_TABLE", "dynamocache-example"),
    endpoint=os.environ.get("DAX_ENDPOINT") or None,
)


@cache.memoize(ttl_seconds=300)
def compute_report(customer_id: str) -> dict:
    """Stand-in for an expensive query or downstream API call."""
    # Imagine a multi-second aggregation here.
    return {"customer_id": customer_id, "score": len(customer_id) * 7}


def handler(event, context):
    customer_id = event.get("customer_id", "anonymous")
    report = compute_report(customer_id)
    return {"statusCode": 200, "body": report}


if __name__ == "__main__":
    # Local smoke test (direct DynamoDB mode). Requires AWS credentials and,
    # on first run, an existing table — call cache.create_table() once.
    print(handler({"customer_id": "cust-123"}, None))
