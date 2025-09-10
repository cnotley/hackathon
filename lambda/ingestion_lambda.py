import os, json, logging, time
from layers.common.python.common import client

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

MAX_MB = int(os.environ.get("MAX_UPLOAD_MB", "5"))


def validate_file_size(size_bytes: int) -> None:
    """Validate the inbound object's size.

    Parameters
    ----------
    size_bytes: int
        Size of the object in bytes.

    Raises
    ------
    ValueError
        If the file is larger than the configured ``MAX_UPLOAD_MB`` limit.
    """

    size_mb = size_bytes / (1024 * 1024)
    if size_mb > MAX_MB:
        raise ValueError(
            f"File too large ({size_mb:.2f}MB) > {MAX_MB}MB limit"
        )


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
    for attempt in range(5):
        try:
            sfn.start_execution(stateMachineArn=arn, input=json.dumps(input_payload))
            return {
                "status": "started",
                "stateMachineArn": arn,
                "attempt": attempt + 1,
            }
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
        size = rec["s3"]["object"].get("size") or 0
        if not size:
            s3 = client("s3")
            try:
                head = s3.head_object(Bucket=b, Key=k)
                size = head.get("ContentLength", 0)
            except Exception as e:
                logger.warning("HEAD failed for s3://%s/%s: %s", b, k, e)
                size = 0
        if not size:
            msg = "unknown_size"
            logger.error("Missing or zero size for s3://%s/%s", b, k)
            results.append({"bucket": b, "key": k, "error": msg})
            try:
                client("s3").put_object_tagging(
                    Bucket=b,
                    Key=k,
                    Tagging={"TagSet": [{"Key": "status", "Value": msg}]},
                )
            except Exception as tag_err:  # pragma: no cover - best effort
                logger.warning("tagging failed for %s/%s: %s", b, k, tag_err)
            continue
        try:
            validate_file_size(int(size))
        except ValueError as e:
            msg = str(e)
            logger.error(msg)
            results.append({"bucket": b, "key": k, "error": msg})
            # attempt to tag object as rejected
            try:
                client("s3").put_object_tagging(
                    Bucket=b,
                    Key=k,
                    Tagging={"TagSet": [{"Key": "status", "Value": "rejected"}]},
                )
            except Exception as tag_err:  # pragma: no cover - best effort
                logger.warning("tagging failed for %s/%s: %s", b, k, tag_err)
            continue
        logger.info("Accepting %s/%s size=%.2fMB", b, k, int(size) / (1024 * 1024))
        start = _start_workflow(b, k)
        # tag object to show workflow started
        try:
            client("s3").put_object_tagging(
                Bucket=b,
                Key=k,
                Tagging={"TagSet": [{"Key": "status", "Value": start.get("status")}]} ,
            )
        except Exception as tag_err:  # pragma: no cover - best effort
            logger.warning("tagging failed for %s/%s: %s", b, k, tag_err)
        results.append({"bucket": b, "key": k, "start": start})
    return {"batch": results}
