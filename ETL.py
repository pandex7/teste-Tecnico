import os
from requests import get
import pandas as pd
from google.cloud import storage
import re
import json
import unidecode
from google.cloud import secretmanager
import logging


def secret_manager(secret_resource_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=secret_resource_id)
        secret_value = response.payload.data.decode("UTF-8")
        return secret_value
    except Exception as e:
        logging.error(f"Failed to access secret: {secret_resource_id}, due to: {e}")
        raise e


def clean_field_name(field_name):
    no_accents = unidecode.unidecode(field_name)
    cleaned_name = re.sub(r"[^a-zA-Z0-9_]", "", no_accents)
    if cleaned_name == "RetornoouRevisao":
        cleaned_name = "RevisaoouRetorno"  # Substitui "RetornoouRevisao" por "RevisaoouRetorno"
    return cleaned_name


def call_api(request):
    print(request)
    request_json = request.get_json()
    endpoint = request_json["endpoint"]
    print(endpoint)
    env = os.environ.get("env")
    secret = os.environ.get("secret")

    try:
        token = secret_manager(f"projects/{secret}/secrets/baseb_token/versions/latest")
    except Exception as e:
        return f"Failed to access secret: {e}", 500

    if not token:
        return "Token de API não definido", 500

    headers = {"Authorization": f"Bearer {token}"}
    print("Iniciando requisições")

    messages = []
    try:
        # URL não existe, apenas teste.
        api_url = f"https://api.marketplace.app/v1/{endpoint}"
        print(f"Chamando endpoint {endpoint} em {api_url}")

        # Simulação de dados de resposta
        response = [
            {
                "id": "evt-123456789",
                "timestamp": "2024-04-25T14:30:00Z",
                "text": "Adoro as joias deste marketplace! Sempre encontro peças únicas e de ótima qualidade.",
                "sentiment": {
                    "score": 0.95,
                    "label": "positivo"
                },
                "product": {
                    "id": "prod-987654321",
                    "name": "Bracelete Artesanal de Prata"
                },
                "user": {
                    "id": "usr-123456789",
                    "username": "artlover"
                },
                "source": "Twitter"
            },
            {
                "id": "evt-987654321",
                "timestamp": "2024-04-26T10:15:00Z",
                "text": "Muito decepcionado com o serviço de entrega deste marketplace, demorou semanas e o produto chegou danificado.",
                "sentiment": {
                    "score": 0.2,
                    "label": "negativo"
                },
                "product": {
                    "id": "prod-123456789",
                    "name": "Vaso de Cerâmica Pintado à Mão"
                },
                "user": {
                    "id": "usr-987654321",
                    "username": "ceramicfan"
                },
                "source": "Facebook"
            }
        ]

        data = response
        print("Data Before Cleaning: ", data)

        converted_data = []
        for item in data:
            if isinstance(item, dict):
                custom_fields = item.pop('customFields', None)
                cleaned_item = {clean_field_name(k): v for k, v in item.items()}

                if custom_fields is not None:
                    if isinstance(custom_fields, dict):
                        cleaned_item['customFields'] = {clean_field_name(k): v for k, v in custom_fields.items()}
                    else:
                        print(f"Unexpected 'customFields' in item {item}: {custom_fields}")
                converted_data.append(cleaned_item)
            else:
                print(f"Unexpected item in data: {item}")

        print("Converted Data: ", converted_data)
        new_line_delimited_json = '\n'.join(json.dumps(item) for item in converted_data)
        print("Converted Data: ", new_line_delimited_json)

        storage_client = storage.Client()
        bucket = storage_client.bucket(env)

        file = endpoint.replace("-", "_")
        diretorio = file.lower()

        blob = bucket.blob(f'land/baseb/{diretorio}/load/{file}.json')
        print(blob)
        blob.upload_from_string(data=new_line_delimited_json, content_type='application/json')

        msg = f"Data uploaded to GCS bucket '{env}' in 'raw/baseb/{endpoint}/'."
        messages.append(msg)
        print(msg)
    except Exception as e:
        error_msg = f"Erro ao processar o endpoint {endpoint} ou ao armazenar o arquivo: {str(e)}"
        messages.append(error_msg)
        print(error_msg)

    return 'End of process!'
