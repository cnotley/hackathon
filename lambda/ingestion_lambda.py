import os, json, logging, time
from layers.common.python.common import client

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

MAX_MB = int(os.environ.get("MAX_UPLOAD_MB", "5"))

def _start_workflow(bucket, key):
    """Attempt to start the Step Functions workflow with retries."""
    sfn = client("stepfunctions")
    input_payload = {"bucket": bucket, "key": key}
    arn = os.environ.get("STATE_MACHINE_ARN")
    if not arn:
        try:
            resp = sfn.list_state_machines()
            if resp.get("stateMachines"):
                arn = resp["stateMachines"][0]["stateMachineArn"]
        except Exception as e:
            logger.warning("Could not list state machines: %s", e)
    if not arn:
        return {"status": "no_state_machine", "input": input_payload}

    delay = 1.0
    for attempt in range(3):
        try:
            sfn.start_execution(stateMachineArn=arn, input=json.dumps(input_payload))
            return {"status": "started", "stateMachineArn": arn, "attempt": attempt + 1}
        except Exception as e:
            logger.warning("start_execution failed (attempt %s): %s", attempt + 1, e)
            time.sleep(delay)
            delay *= 1.5
    return {"status": "error", "error": "start_failed", "input": input_payload}

def handle_event(event, context):
    """S3 ObjectCreated event -> validate size & start Step Functions.
    Reject files larger than MAX_UPLOAD_MB. Batch over multi-record events.
    """
    results = []
    for rec in event.get("Records", []):
        b = rec["s3"]["bucket"]["name"]
        k = rec["s3"]["object"]["key"]
        size = rec["s3"]["object"].get("size") or rec["s3"]["object"].get("sequencer") or 0
        if not size:
            s3 = client("s3")
            try:
                head = s3.head_object(Bucket=b, Key=k)
                size = head.get("ContentLength", 0)
            except Exception as e:
                logger.warning("HEAD failed for s3://%s/%s: %s", b, k, e)
                size = 0
        size_mb = (int(size) / (1024*1024))
        if size_mb > MAX_MB:
            msg = f"File too large ({size_mb:.2f}MB) > {MAX_MB}MB limit"
            logger.error(msg)
            results.append({"bucket": b, "key": k, "error": msg})
            continue
        logger.info("Accepting %s/%s size=%.2fMB", b, k, size_mb)
        start = _start_workflow(b, k)
        results.append({"bucket": b, "key": k, "start": start})
    return {"batch": results}
