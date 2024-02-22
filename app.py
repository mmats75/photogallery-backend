from chalice import Chalice
from chalicelib.aws_util import *

app = Chalice(app_name="backend")


@app.route("/", cors=True)
def index():
    return {"hello": "world"}


# Function to return photo metadata for designated album from DynamoDB
@app.route("/album/{category}", cors=True)
def read_metadata(category):
    print("category:", category)

    # DynamoDB table parameters
    table_name = "photo-metadata"
    partition_key_name = "type"
    partition_key_value = "photo"
    filter_expression = "category = :sortkeyval"
    expression_attribute_values = {":sortkeyval": {"S": category}}
    projection_expression = "s3_object_name, taken_at, category, tags"

    # Query operation
    items = query_items_by_partition_key(
        table_name,
        partition_key_name,
        partition_key_value,
        True,
        filter_expression,
        expression_attribute_values,
        projection_expression,
    )
    print(len(items), "items")

    # Convert items
    converted_items = [convert_DynamoDB_format_to_dict(item) for item in items]
    print(converted_items[0])

    return {"items": converted_items}
