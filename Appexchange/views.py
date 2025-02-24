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
            request_data = json.loads(request.body)

            if not isinstance(request_data, list):
                return JsonResponse({"error": "Invalid input. Expected a list of objects."}, status=400)

            response_data = {"objects": []}
            for obj in request_data:
                if "sObject" not in obj or "fieldMap" not in obj:
                    return JsonResponse({"error": "Invalid object structure."}, status=400)

                selection_criteria = obj.get("filter", "")

                data_source = "source" if "Account" in obj["sObject"] else "target"

                fields = "SELECT " + ", ".join(
                    field['sourceFld'] for field in obj["fieldMap"]
                )

                mapped_object = {
                    "object": obj["sObject"].lower(),
                    "fields": fields,
                    "selectionCriteria": f"where {selection_criteria}" if selection_criteria else "",
                    "dataSource": data_source,
                    "pkChunking": False
                }

                response_data["objects"].append(mapped_object)

            json_data = json.dumps(response_data, indent=4)

            file_name = f"mapped_data_{str(uuid.uuid4())}.json"

            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=str(map_fields),  
                Body=json_data.encode("utf-8"), 
                ContentType="application/json"
            )
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

@csrf_exempt
def generate_account_mapping(request):
    if request.method == "POST":
        try:
            body = json.loads(request.body.decode("utf-8"))

            if isinstance(body, dict) and "objects" in body:  
                body = body["objects"]  

            if not isinstance(body, list):
                return JsonResponse({"error": "Invalid input format, expected a list."}, status=400)

            account_mapping = None

            for obj in body:
                obj_name = obj.get("object") or obj.get("sObject")  
                
                if obj_name and obj_name.lower() == "account":  
                    fields_str = obj.get("fields", "")  
                    if fields_str.startswith("SELECT "):
                        account_mapping = fields_str.replace("SELECT ", "").split(", ")
                    break  

            if not account_mapping:
                return JsonResponse({"error": "No Account mappings found"}, status=400)

            mapping_lines = [f"{field.strip()}={field.strip()}" for field in account_mapping]  
            mapping_content = "\n".join(mapping_lines)

            file_key = f"account_mapping_{uuid.uuid4()}.txt"

            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=file_key,  
                Body=mapping_content.encode("utf-8"),
                ContentType="text/plain"
            )
           

            s3_url = f"https://rh-data-migration-json-data.s3.amazonaws.com/generate_account_mapping"
            return JsonResponse({"message": "Account mapping file generated successfully", "s3_url": s3_url}, status=201)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON format"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    return JsonResponse({"error": "Invalid request method. Use POST."}, status=405)