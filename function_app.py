import azure.functions as func
import json
import logging
import os
from azure.identity import ManagedIdentityCredential
from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient

# Configurações da Storage Account
storage_account_name = "moutest"
queue_name = "input"
container_name = "work"

# Inicialização dos clientes
credential = ManagedIdentityCredential()
queue_url = f"https://{storage_account_name}.queue.core.windows.net/{queue_name}"
queue_client = QueueClient.from_queue_url(queue_url=queue_url, credential=credential)
blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)

app = func.FunctionApp()

@app.function_name(name="HttpTriggerFunction")
@app.route(route="use_identity/{action}")  # O caminho da URL no qual a função estará disponível
def use_identity(req: func.HttpRequest, action: str) -> func.HttpResponse:
    logging.info(f'Python HTTP trigger function processed a request. Action: {action}')
    try:
        if action == "send-message":
            message = req.params.get('message')
            if not message:
                return func.HttpResponse("Please provide a 'message' query parameter.", status_code=400)
            result = send_message(message)
            return func.HttpResponse(result)

        elif action == "process-messages":
            num_messages = int(req.params.get('num', 5))
            result = process_messages(num_messages)
            return func.HttpResponse(json.dumps(result))

        elif action == "list-blobs":
            result = list_blobs()
            return func.HttpResponse(json.dumps(result))

        elif action == "download-blob":
            blob_name = req.params.get('blob')
            if not blob_name:
                return func.HttpResponse("Please provide a 'blob' query parameter.", status_code=400)
            result = download_blob(blob_name)
            return func.HttpResponse(result)

        else:
            return func.HttpResponse("Invalid action specified.", status_code=400)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)

# Função para enviar uma mensagem para a fila
def send_message(queue_client, message_content):
    print(f"Enviando mensagem: {message_content}")
    queue_client.send_message(message_content)

# Função para ler e processar mensagens da fila
def process_messages(num_messages=5):
    messages = queue_client.receive_messages(messages_per_page=num_messages)
    
    for message in messages:
        print(f"Mensagem recebida: {message.content}")
        queue_client.delete_message(message)

# Função para listar blobs em um container
def list_blobs():
    container_client = blob_service_client.get_container_client(container_name)
    print(f"\nBlobs no container '{container_name}':")
    for blob in container_client.list_blobs():
        print(f" - {blob.name}")

# Função para baixar um arquivo de um blob
def download_blob(blob_name: str, download_file_path: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    print(f"\nBaixando blob '{blob_name}' para '{download_file_path}'...")
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    print("Download concluído!")

def send_message(message_content):
    queue_client.send_message(message_content)
    return f"Mensagem enviada: {message_content}"

def process_messages(num_messages=5):
    messages = queue_client.receive_messages(messages_per_page=num_messages)
    processed = []
    for message in messages:
        processed.append(message.content)
        queue_client.delete_message(message)
    return processed

def list_blobs():
    container_client = blob_service_client.get_container_client(container_name)
    return [blob.name for blob in container_client.list_blobs()]

def download_blob(blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    return blob_client.download_blob().readall().decode()
