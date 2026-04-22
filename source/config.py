import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

APP_ENVIRONMENT = os.getenv("APP_ENVIRONMENT", None)
LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
# region PATHs
EXCEL_SHEETS_TAGGING_RESULTS_FOLDER_PATH = os.getenv(
    "EXCEL_SHEETS_TAGGING_RESULTS_FOLDER_PATH", None
)
EXCEL_SHEETS_GROUP_DF_FINDING_TEMPLATE_COLUMN_RESULTS_FOLDER_PATH = os.getenv(
    "EXCEL_SHEETS_GROUP_DF_FINDING_TEMPLATE_COLUMN_RESULTS_FOLDER_PATH", None
)
PRE_FINAL_TEMPLATE_TIPO_PAGO_HOMOLOGATION_RESULTS_FOLDER_PATH = os.getenv(
    "PRE_FINAL_TEMPLATE_TIPO_PAGO_HOMOLOGATION_RESULTS_FOLDER_PATH", None
)
# endregion
# region Parametros de ejecución de los modelos
AZURE_OPENAI_EMBEDDINGS_DIMENSION = int(
    os.getenv("AZURE_OPENAI_EMBEDDINGS_DIMENSION", "1024")
)

# endregion

# region Variables de entorno Azure OpenAI
# Azure OpenAI Embeddings
AZURE_OPENAI_EMBEDDINGS_MODEL_NAME = os.getenv("AZURE_OPENAI_EMBEDDINGS_MODEL_NAME")

# Azure OpenAI LLM
AZURE_OPENAI_LLM_MODEL_NAME = os.getenv("AZURE_OPENAI_LLM_MODEL_NAME")

# endregion

# region SecureGPT
AXA_SECURE_GPT_CLIENT_ID = os.getenv("AXA_SECURE_GPT_CLIENT_ID")
AXA_SECURE_GPT_CLIENT_SECRET = os.getenv("AXA_SECURE_GPT_CLIENT_SECRET")
AXA_SECURE_GPT_ONE_ACCOUNT_URL = os.getenv("AXA_SECURE_GPT_ONE_ACCOUNT_URL")
AXA_SECURE_GPT_BASE_ENDPOINT = os.getenv("AXA_SECURE_GPT_BASE_ENDPOINT")
AXA_SECURE_GPT_OPENAI_API_VERSION = os.getenv("AXA_SECURE_GPT_OPENAI_API_VERSION")
AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID = os.getenv("AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID")
AXA_SECURE_GPT_LLM_ID = os.getenv("AXA_SECURE_GPT_LLM_ID")
AXA_SECURE_GPT_OPENAI_BASE_EMBEDDINGS_ENDPOINT = os.getenv(
    "AXA_SECURE_GPT_OPENAI_BASE_EMBEDDINGS_ENDPOINT"
)
AXA_SECURE_GPT_OPENAI_BASE_LLM_ENDPOINT = os.getenv(
    "AXA_SECURE_GPT_OPENAI_BASE_LLM_ENDPOINT"
)
# endregion

# region descuentos por tipo de pago y aseguradora
PAYMENT_TYPE_INSURANCE_DISCOUNTS_DICT = {
    "Sin desglose": {
        "GNP": 0.115,
        "Metlife": 0.113,
        "SMNYL": 0.1006,
        "Atlas": 0.0961,
        "Zurich": 0.0904,
        "Banorte": 0.0904,
        "Inbursa": 0.0904,
        "Mapfre": 0.0904,
        "BUPA México": 0.0904,
        "Allianz México": 0.0904,
        "Ve por Más": 0.0904,
    },
    "Pago directo": {
        "GNP": 0.13,
        "Metlife": 0.128,
        "SMNYL": 0.1139,
        "Atlas": 0.1088,
        "Zurich": 0.1024,
        "Banorte": 0.1024,
        "Inbursa": 0.1024,
        "Mapfre": 0.1024,
        "BUPA México": 0.1024,
        "Allianz México": 0.1024,
        "Ve por Más": 0.1024,
    },
    "Reembolso": {
        "GNP": 0,
        "Metlife": 0,
        "SMNYL": 0,
        "Atlas": 0,
        "Zurich": 0,
        "Banorte": 0,
        "Inbursa": 0,
        "Mapfre": 0,
        "BUPA México": 0,
        "Allianz México": 0,
        "Ve por Más": 0,
    },
}

TAGGING_EXCEL_SHEET_DF_LLM_OUTPUT_DICT_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning_process": {"type": "string"},
        "dataset_tag": {"type": "string"},
    },
    "required": ["reasoning_process", "dataset_tag"],
    "additionalProperties": False,
}

FIND_TEMPLATE_FIELD_LLM_OUTPUT_DICT_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning_process": {"type": "string"},
        "matched_column_name": {"type": "string"},
    },
    "required": ["reasoning_process", "matched_column_name"],
    "additionalProperties": False,
}

HOMOLOGATE_VALUES_USING_MAPPING_DICT_LLM_OUTPUT_DICT_SCHEMA = {
    "type": "object",
    "properties": {
        "reasoning_process": {"type": "string"},
        "grouping_dict": {"type": "object"},
    },
    "required": ["reasoning_process", "grouping_dict"],
    "additionalProperties": False,
}


FINAL_SINISTER_TEMPLATE_COLUMN_NAMES = [
    "excel_file_name",
    "excel_file_sheet_name",
    "POLIZA",
    "SINIESTRO",
    "Reclamación",
    "ASEGURADORA",
    "Sexo",
    "Edad",
    "Claveenfermedad",
    "Clave ICD",
    "Tipo Siniestro",
    "Padecimiento",
    "Fecha de Pago",
    "Fecha de 1er Gasto",
    "Tipo de Pago",
    "Monto Pagado Reportado",
    "Pagos",
    "IVA",
    "%descuento aplicado",
    "Tipo de producto",
    "Tipo de Proveedor",
    "Deducible",
    "Coaseguro ",
]

FINAL_CENSUS_TEMPLATE_COLUMN_NAMES = [
    "excel_file_name",
    "excel_file_sheet_name",
    "Póliza",
    "Subgrupo",
    "Numero Sgrp",
    "Certificado",
    "Parentesco",
    "Género",
    "Fecha de nacimiento",
    "Fecha de ingreso",
    "Fecha de antigüedad",
]

MAX_ROWS_FROM_DF_TO_TAG_EXCEL_SHEET_INT = 15
MAX_ROWS_FROM_DF_TO_TAG_EXCEL_SHEET_GROUP_INT = 15
MAX_ROWS_FROM_DF_TO_MAP_EXCEL_FIELD_INT = 15
MAX_EXCEL_FILES_CONCURRENCY = 2
