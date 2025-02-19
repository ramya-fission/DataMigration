import boto3
import json
import uuid
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt


AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
S3_BUCKET_NAME = "rh-data-migration-json-data"


s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

@csrf_exempt
def map_fields(request):
    if request.method == "POST":
        try:
            # Parse incoming JSON request
            request_data = json.loads(request.body)

            # Validate input
            if not isinstance(request_data, list):
                return JsonResponse({"error": "Invalid input. Expected a list of objects."}, status=400)

            # Process each object and generate the desired mapping
            response_data = {"objects": []}
            for obj in request_data:
                if "sObject" not in obj or "fieldMap" not in obj:
                    return JsonResponse({"error": "Invalid object structure."}, status=400)

                # Get the selection criteria (filter) or use an empty string if not provided
                selection_criteria = obj.get("filter", "")

                # Determine the dataSource based on sObject (source or target)
                data_source = "source" if "Account" in obj["sObject"] else "target"

                # Generate field selection
                fields = "SELECT " + ", ".join(
                    field['sourceFld'] for field in obj["fieldMap"]
                )

                # Construct mapped object
                mapped_object = {
                    "object": obj["sObject"].lower(),
                    "fields": fields,
                    "selectionCriteria": f"where {selection_criteria}" if selection_criteria else "",
                    "dataSource": data_source,
                    "pkChunking": False
                }

                response_data["objects"].append(mapped_object)

            # Convert response data to JSON string
            json_data = json.dumps(response_data, indent=4)

            # Generate unique filename and ensure it is a string
            file_name = f"mapped_data_{str(uuid.uuid4())}.json"

            # Upload JSON to S3
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=str(map_fields),  # Ensure Key is a string
                Body=json_data.encode("utf-8"),  # Convert JSON string to bytes
                ContentType="application/json"
            )

            # Generate file URL
            s3_url = f"https://rh-data-migration-json-data.s3.amazonaws.com/map_fields)"

            return JsonResponse(
                {"message": "File uploaded successfully", "s3_url": s3_url},
                status=200
            )

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON format."}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Invalid request method. Use POST."}, status=405)
