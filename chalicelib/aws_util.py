import boto3
from botocore.exceptions import ClientError


def convert_DynamoDB_format_to_dict(item, inner=False):
    if not inner:
        return {k: convert_DynamoDB_format_to_dict(v, True) for k, v in item.items()}
    for k, v in item.items():
        if k == "L":
            return [convert_DynamoDB_format_to_dict(val, True) for val in v]
        if k == "M":
            ret = {s: convert_DynamoDB_format_to_dict(t, True) for s, t in v.items()}
            return ret
        return v


def query_items_by_partition_key(
    table_name,
    partition_key_name,
    partition_key_value,
    sort_key_order_ascending=True,
    filter_expression=None,
    expression_attribute_values=None,
    projection_expression=None,
):
    """
    Query items in a DynamoDB table with optional filtering and projection, and sorting by the sort key.

    :param table_name: Name of the DynamoDB table.
    :param partition_key_name: Name of the partition key.
    :param partition_key_value: Value of the partition key to query.
    :param sort_key_order_ascending: Sort order (True for ascending, False for descending).
    :param filter_expression: Optional filter expression to apply to the query.
    :param expression_attribute_values: Optional dictionary of expression attribute values.
    :param projection_expression: Optional string specifying the attributes to be retrieved.
    :return: List of items from the query operation.
    """
    # Create a DynamoDB client
    dynamodb = boto3.client("dynamodb")  # , region_name='ap-northeast-1')

    # Prepare the ExpressionAttributeValues for the query
    expression_attribute_values = expression_attribute_values or {}
    expression_attribute_values.update({":pkval": {"S": partition_key_value}})

    query_params = {
        "TableName": table_name,
        "KeyConditionExpression": "#pk = :pkval",
        "ExpressionAttributeNames": {"#pk": partition_key_name},
        "ExpressionAttributeValues": expression_attribute_values,
        "ScanIndexForward": sort_key_order_ascending,
    }

    # Add filter expression if provided
    if filter_expression:
        query_params["FilterExpression"] = filter_expression

    # Add projection expression if provided
    if projection_expression:
        query_params["ProjectionExpression"] = projection_expression

    try:
        response = dynamodb.query(**query_params)
        return response["Items"]
    except ClientError as e:
        print("An error occurred:", e.response["Error"]["Message"])
        return None
