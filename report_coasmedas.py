import pandas as pd
from datetime import datetime
from status_core_id import get_status_core_id
from connection import get_registers_collection, get_registershflow_collection

from fastapi import FastAPI, Request, HTTPException
import httpx

app = FastAPI()

# Obtener colecciones
registers = get_registers_collection()
registershflow = get_registershflow_collection()

# Entrada del clientId y las fechas por los que desea filtrar por metodo POST
@app.post("/receive_data")
async def receive_data(request: Request):
    try:
        data = await request.json()
        if 'userId' in data:
            userId = data['userId']
            init_date = datetime(data['initDate'])
            end_date = datetime(data['endDate'])
            return searching(userId, init_date, end_date)
        else:
            raise HTTPException(status_code=400, detail="El campo 'ID' es requerido")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Campos que se quieren traer de la colección registers
coreid_fields = {
    "_id": 1,
    "clientId": 1,
    "startDatetime": 1,
    "userDocument.documentNumber": 1,
    "userDocument.firstName": 1,
    "userDocument.lastName": 1,
    "email": 1,
    "phone": 1,
    "capturedImages.docFront": 1,
    "wasRejected": 1,
    "unifiedChecksOk": 1,
    "result": 1,
}

sflow_fields = {
    "userId": 1,
    "currentAssignees": 1,  # Incluir la columna completa para entender su estructura
    "executedSteps.FORMULARIO": 1,
    "currentStepId": 1,
    "acceptanceStatus": 1,
}

def search_coreid(docsfiltered, registershflow):
    data = []
    for doc in docsfiltered:
        data_doc = {}
        for field in coreid_fields:
            levels = field.split(".")
            valor = doc
            for level in levels:
                valor = valor.get(level, None)
                if valor is None:
                    break
            data_doc[field] = valor if valor is not None else ""
        data_doc['status_core_id'] = get_status_core_id(doc).value

        # Obtener el _id desde registers
        idcoreid = str(doc.get("_id", ""))

        # Obtener datos de registershflow usando el _id de registers
        try:
            data_from_hflow = registershflow.find_one({"userId": idcoreid}, sflow_fields)
        except Exception as e:
            print(f"Error fetching data from registershflow: {e}")
            print(idcoreid)

        # Agregar datos de registershflow al DataFrame si se encontró el documento
        if data_from_hflow:
            data_doc['CurrentStepId'] = data_from_hflow.get('currentStepId', '')
            data_doc['AcceptanceStatus'] = data_from_hflow.get('acceptanceStatus', '')
            data_doc['CoasmedasValorPago'] = data_from_hflow.get('executedSteps', {}).get('FORMULARIO', {}).get('resultData', {}).get('coasmedas.valorPago', '')

        data.append(data_doc)

    return data

# Obtener datos de registers
def searching(cid, init_date, end_date):
    docsfiltered = registers.find({
        "clientId": cid,
        "startDatetime": {'$gte': init_date, '$lt': end_date}
        },
        coreid_fields)

    try:
        data = search_coreid(docsfiltered, registershflow)
    except Exception as e:
        print(f"Error in search_coreid function: {e}")
    else:
        df = pd.DataFrame(data)

        # Renombrar las columnas
        column_rename_mapping = {
            "_id": "ID",
            "startDatetime": "Fecha registro",
            "userDocument.documentNumber": "Numero documento",
            "userDocument.firstName": "Nombres",
            "userDocument.lastName": "Apellidos",
            "email": "Email",
            "phone": "No. Celular",
            "status_core_id": "Estado identidad digital",
            "userId": "Id hflow",
            "CurrentStepId": "Current Step Id",
            "AcceptanceStatus": "Acceptance Status",
            "CoasmedasValorPago": "Coasmedas Valor Pago"
        }

        df.rename(columns=column_rename_mapping, inplace=True)

        export_route = "Informe_Coasmedas.xlsx"
        exclude_fields = ["clientId", "capturedImages.docFront", "wasRejected", "expirationDate", "unifiedChecksOk", "result"]
        df.drop(exclude_fields, axis=1, errors='ignore', inplace=True)
        df.to_excel(export_route, index=False)

        print(f"Data exported correctly to {export_route} for clientid {cid}")
    finally:
        print("OK")
