import azure.functions as func
import json
import logging

app = func.FunctionApp()

@app.function_name(name="HttpTriggerFunction")
@app.route(route="export_shape")  # O caminho da URL no qual a função estará disponível
def export_shape(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function executed.')
    
    response_message = {
        'message': 'Export shape function executed successfully!'
    }

    return func.HttpResponse(
        json.dumps(response_message),
        mimetype="application/json",
        status_code=200
    )
