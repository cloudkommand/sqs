import boto3
import botocore
# import jsonschema
import json
import traceback

from requests.utils import quote
from botocore.exceptions import ClientError

from extutil import remove_none_attributes, account_context, ExtensionHandler, \
    ext, component_safe_name, handle_common_errors

eh = ExtensionHandler()

sqs = boto3.client("sqs")
def lambda_handler(event, context):
    try:
        print(f"event = {event}")
        account_number = account_context(context)['number']
        region = account_context(context)['region']
        eh.capture_event(event)
        prev_state = event.get("prev_state")
        cdef = event.get("component_def")
        cname = event.get("component_name")
        project_code = event.get("project_code")
        repo_id = event.get("repo_id")
        fifo = cdef.get("fifo")
        
        queue_name = cdef.get("name") or \
            (
                component_safe_name(project_code, repo_id, cname, max_chars=80) 
                    if not fifo else
                f"{component_safe_name(project_code, repo_id, cname, max_chars=75)}.fifo"
            )
        
        delay_seconds = cdef.get("delay_seconds") or 0
        maximum_message_size = 262144
        retention_seconds = cdef.get("retention_seconds") or 345600
        dead_letter_queue_arn = cdef.get("dead_letter_queue_arn")
        max_count_before_dead_letter = cdef.get("max_count_before_dead_letter")
        policy = cdef.get("policy")
        visibility_timeout = cdef.get("visibility_timeout", 30)

        kms_key_id = cdef.get("kms_key_id")
        kms_key_reuse_seconds = cdef.get("kms_key_reuse_seconds") or (300 if kms_key_id else None)
        sqs_managed_sse = cdef.get("sqs_managed_sse")

        content_based_deduplication = cdef.get("content_based_deduplication")
        deduplication_scope = cdef.get("deduplication_scope")
        fifo_throughput_limit = cdef.get("fifo_throughput_limit")

        tags = cdef.get("tags") or {}

        pass_back_data = event.get("pass_back_data", {})
        if pass_back_data:
            pass
        elif event.get("op") == "upsert":
            old_queue_name = None
            old_queue_url = None
            try:
                old_queue_name = prev_state["props"]["name"]
                old_queue_url = prev_state["props"]["url"]
            except:
                pass
            
            eh.add_op("get_queue_url")
            if old_queue_name and queue_name != old_queue_name:
                eh.add_op("delete_queue", {"url":old_queue_url, "only_delete": False})

        elif event.get("op") == "delete":
            eh.add_op("delete_queue", {"url":prev_state.get("props", {}).get("url"), "only_delete": True})
        
        attributes = remove_none_attributes({
            "DelaySeconds": delay_seconds,
            "MaximumMessageSize": maximum_message_size,
            "MessageRetentionPeriod": retention_seconds,
            "Policy": json.dumps(policy) if policy else None,
            "RedrivePolicy": remove_none_attributes({
                "deadLetterTargetArn": dead_letter_queue_arn,
                "maxReceiveCount": str(max_count_before_dead_letter) if max_count_before_dead_letter else None
            }),
            "VisibilityTimeout": visibility_timeout,
            "ContentBasedDeduplication": content_based_deduplication,
            "DeduplicationScope": deduplication_scope,
            "FifoQueue": fifo,
            "FifoThroughputLimit": fifo_throughput_limit,
            "KmsMasterKeyId": kms_key_id,
            "KmsDataKeyReusePeriodSeconds": kms_key_reuse_seconds,
            "SqsManagedSseEnabled": sqs_managed_sse
        })

        attributes = {k:str(v) for k,v in attributes.items() if not isinstance(v, dict)}
        print(attributes)

        get_queue_url(queue_name, account_number, region)
        get_queue(attributes, tags)
        create_queue(queue_name, account_number, region, attributes, tags)
        set_queue_attributes(attributes, tags)        
        remove_tags()
        add_tags()
        delete_queue()
            
        return eh.finish()

    except Exception as e:
        msg = traceback.format_exc()
        print(msg)
        eh.add_log("Uncovered Error", {"error": msg}, is_error=True)
        eh.declare_return(200, 0, error_code=str(e))
        return eh.finish()

@ext(handler=eh, op="get_queue_url")
def get_queue_url(queue_name, account_number, region):
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        eh.add_state({"queue_url": response["QueueUrl"]})
        eh.add_op("get_queue")

        eh.add_props({
            "arn": gen_sqs_queue_arn(queue_name, account_number, region),
            "url": response["QueueUrl"],
            "name": queue_name
        })

        eh.add_links({
            "Queue": gen_sqs_queue_link(region, response["QueueUrl"])
        })
    except ClientError as e:
        if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
            eh.add_op("create_queue")
        else:
            handle_common_errors(e, eh, "Get Queue Url Failure", 0)

@ext(handler=eh, op="get_queue")
def get_queue(attributes, tags):
    queue_url = eh.state["queue_url"]

    try:
        response = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        eh.add_log("Got Queue Attributes", response)
        current_attributes = response["Attributes"]
        print(f"current_attributes = {current_attributes}")
        print(f"attributes = {attributes}")
        for k,v in attributes.items():
            if str(current_attributes.get(k)).lower() != str(v).lower():
                eh.add_op("set_queue_attributes")
                print(k)
                print(v)
                print(type(k))
                print(type(v))
                break

        if not eh.ops.get("set_queue_attributes"):
            eh.add_log("Nothing to do. Exiting", {"current_attributes": current_attributes, "desired_attributes": attributes})

    except ClientError as e:
        if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
            eh.add_op("create_queue")
        else:
            handle_common_errors(e, eh, "Get Queue Attributes Failure", 0)

    try:
        tags_response = sqs.list_queue_tags(QueueUrl=queue_url)
        current_tags = tags_response.get("Tags") or {}
    except ClientError as e:
        handle_common_errors(e, eh, "List Queue Tags Error", 10)

    if tags != current_tags:
        remove_tags = [k for k in current_tags.keys() if k not in tags]
        add_tags = {k:v for k,v in tags.items() if k not in current_tags.keys()}
        if remove_tags:
            eh.add_op("remove_tags", remove_tags)
        if add_tags:
            eh.add_op("add_tags", add_tags)


@ext(handler=eh, op="create_queue")
def create_queue(queue_name, account_number, region, attributes, tags):
    try:
        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes=attributes,
            tags=tags
        )
        eh.add_log("Created Queue", response)
        eh.add_op("get_queue_arn")
        eh.add_props({
            "name": queue_name,
            "url": response["QueueUrl"],
            "arn": gen_sqs_queue_arn(queue_name, account_number, region)
        })

        eh.add_links({
            "Queue": gen_sqs_queue_link(region, response["QueueUrl"])
        })

    except botocore.exceptions.ParamValidationError as e:
        eh.add_log("Invalid Create Queue Parameters", {"error": str(e)}, is_error=True)
        eh.perm_error(str(e), 20)

    except ClientError as e:
        handle_common_errors(e, eh, "Error Creating Queue", progress=20)

@ext(handler=eh, op="set_queue_attributes")
def set_queue_attributes(attributes, tags):
    queue_url = eh.state["queue_url"]

    try:
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes=attributes
        )

        eh.add_log("Set Queue Attributes", attributes)
    
    except ClientError as e:
        handle_common_errors(e, eh, "Set Queue Attributes Failure", 20)
        
    # eh.complete_op("create_policy")

@ext(handler=eh, op="add_tags")
def add_tags():
    tags = eh.ops.get("add_tags")
    queue_url = eh.state["queue_url"]
    try:
        response = sqs.tag_policy(
            QueueUrl=queue_url,
            Tags=tags
        )
        eh.add_log("Tags Added", response)

    except ClientError as e:
        handle_common_errors(e, eh, "Error Adding Tags", progress=90)
        

@ext(handler=eh, op="remove_tags")
def remove_tags():
    remove_tags = eh.ops['remove_tags']
    queue_url = eh.state["queue_url"]

    try:
        sqs.untag_queue(
            QueueUrl=queue_url,
            TagKeys=remove_tags
        )
        eh.add_log("Tags Removed", {"tags_removed": remove_tags})

    except ClientError as e:
        handle_common_errors(e, eh, "Error Removing Tags", progress=80)

def gen_sqs_queue_arn(queue_name, account_number, region):
    return f"arn:aws:sqs:{region}:{account_number}:{queue_name}"

def gen_sqs_queue_link(region, queue_url):
    return f"https://{region}.console.aws.amazon.com/sqs/v2/home?region={region}#/queues/{quote(queue_url, safe='')}"


@ext(handler=eh, op="delete_queue")
def delete_queue():
    op_info = eh.ops['delete_queue']
    queue_url = op_info['url']
    only_delete = op_info.get("only_delete")

    try:
        sqs.delete_queue(
            QueueUrl = queue_url
        )
        eh.add_log("Queue Deleted", {"queue_url": queue_url})

    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            eh.add_log("Old Queue Doesn't Exist", {"queue_url": queue_url})
        else:
            handle_common_errors(e, eh, "Error Deleting Queue", progress=(95 if only_delete else 20))
