import asyncio
import json
import time
from io import BytesIO

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from ..config import MAX_EXCEL_FILES_CONCURRENCY, PAYMENT_TYPE_INSURANCE_DISCOUNTS_DICT
from ..core.workflow import (
    gather_parse_and_structure_census_and_sinisters_templates_data_from_an_excel_file,
)
from ..utils.logger import get_logger

logger = get_logger()

# Create FastAPI instance
app = FastAPI(
    title="Data Prep App",
    description="Powered by SecureGPT",
    version="0.1.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_file_data_dict_using_upload_file(
    uploaded_file: UploadFile,
) -> dict[str, str | bytes | None]:
    """
    Obtiene un diccionario con los datos de un archivo recibido vía UploadFile y valida
    que el archivo sea de Excel (XLS o XLSX). Si el archivo no es de Excel, se lanza
    ValueError indicando la restricción.

    Args:
        uploaded_file (UploadFile): Archivo recibido (por ejemplo, en una ruta de API o formulario).
            Debe contener:
                - filename (str): nombre del archivo, que puede incluir la extensión.
                - content_type (str | None): tipo MIME reportado por el cliente. Puede ser None.
                - read(): método asíncrono para leer todo el contenido en bytes.

    Returns:
        dict[str, str | bytes | None]:
            Diccionario con la información del archivo:
                - "file_name": nombre original del archivo (str).
                - "file_bytes": contenido del archivo en bytes (bytes).
                - "file_mime_type": tipo MIME del archivo; puede ser None si no se proporcionó.

    Raises:
        ValueError: si el archivo no es de Excel (no tiene extensión .xls/.xlsx
            o su MIME type no corresponde a un Excel).
            En particular, se considera válido cualquiera de las siguientes combinaciones:
                - Extensión válida: ".xls" o ".xlsx" (ignorando mayúsculas).
                - MIME type válido: "application/vnd.ms-excel" (XLS) o
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" (XLSX).
            Si ninguna condición se cumple, se lanza ValueError con un mensaje claro
            indicando que únicamente se permiten archivos Excel.

    Notas:
        - Se realiza la validación antes de leer el contenido para evitar lecturas innecesarias.
        - Después de la validación, se lee todo el contenido del archivo y se registra
        el tiempo de la operación para métricas.
        - El valor de "file_mime_type" puede ser None si el cliente no provee un MIME type.

    Ejemplo de mensaje de error (cuando aplica):
        ValueError: Archivo no permitido. Solo se permiten archivos Excel (.xls, .xlsx).
    """
    # Validación de tipo de archivo Excel
    filename = (
        uploaded_file.filename
        if uploaded_file and hasattr(uploaded_file, "filename")
        else ""
    )
    ext = ""
    if isinstance(filename, str) and "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()
    mime = uploaded_file.content_type if uploaded_file is not None else None

    is_excel_ext = ext in {"xls", "xlsx"}  # ext sin punto
    mime_excel = mime in {
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    if not (is_excel_ext or mime_excel):
        raise ValueError(
            "Archivo no permitido. Solo se permiten archivos Excel (.xls, .xlsx)."
        )

    logger.info("ARCHIVO RECIBIDO: %s", uploaded_file.filename)
    s_t = time.time()

    # Read all bytes from the uploaded file
    file_bytes = await uploaded_file.read()

    e_t = time.time()
    file_content_type = uploaded_file.content_type  # may be None if not provided

    logger.info(
        "¡ARCHIVO %s LEÍDO! DURACIÓN %s SEGUNDOS",
        uploaded_file.filename,
        f"{e_t - s_t:,.2f}",
    )
    return {
        "file_name": uploaded_file.filename,
        "file_bytes": file_bytes,
        "file_mime_type": file_content_type,  # "excel"
    }


async def process_one_file(
    uploaded_file: UploadFile,
    insurance_company_name: str,
    semaphore: asyncio.Semaphore | None,
) -> dict[str, str | pd.DataFrame]:
    """
    Procesa un único UploadFile: lectura del contenido y
    llamada a la función de extracción/parsing dentro del semáforo.
    """
    s_t = time.time()

    # Lectura de datos del archivo
    file_data_dict = await get_file_data_dict_using_upload_file(uploaded_file)
    excel_file_name = file_data_dict["file_name"]
    excel_file_bytes = file_data_dict["file_bytes"]
    excel_file_mime_type = file_data_dict["file_mime_type"]

    # Aplicar semáforo durante la llamada de procesamiento intensivo
    if semaphore: # pragma: no cover 
        async with semaphore:
            result = await gather_parse_and_structure_census_and_sinisters_templates_data_from_an_excel_file(
                insurance_company_name=insurance_company_name,
                excel_file_name=excel_file_name,
                excel_file_bytes=excel_file_bytes,
                excel_file_mime_type=excel_file_mime_type,
            )

    else:
        result = await gather_parse_and_structure_census_and_sinisters_templates_data_from_an_excel_file(
            insurance_company_name=insurance_company_name,
            excel_file_name=excel_file_name,
            excel_file_bytes=excel_file_bytes,
            excel_file_mime_type=excel_file_mime_type,
        )

    # Cerrar el archivo UploadFile
    await uploaded_file.close()

    e_t = time.time()
    logger.info(
        "ARCHIVO %s PROCESADO EN %s SEGUNDOS",
        excel_file_name,
        "{:,.2f}".format(e_t - s_t),
    )
    return result


@app.post("/gather-and-parse-from-excel-files")
async def gather_and_parse_data_from_many_files(
    folio_id: str = Form(...),
    insurance_company_name: str = Form(...),
    excel_files: list[UploadFile] = File(...),
):
    try:
        valid_insurance_companuy_names = [
            "GNP",
            "Metlife",
            "SMNYL",
            "Atlas",
            "Zurich",
            "Banorte",
        ]
        if insurance_company_name not in valid_insurance_companuy_names:
            raise ValueError(
                f"¡ASEGURADORA INVÁLIDA! Las aseguradoras a las que tengo acceso son:\n{json.dumps(
                    valid_insurance_companuy_names, indent=2, ensure_ascii=False
                )}"
            )
        # Semáforo para limitar la concurrencia de procesamiento de archivos
        # NOTE: Comentado porqué el comportamiendo no fue el esperado. Estudiar, investigar, aplicar correctamente.
        # semaphore = asyncio.Semaphore(MAX_EXCEL_FILES_CONCURRENCY)
        semaphore = None

        # Inicio total
        total_start = time.time()

        # Lanza tareas para todos los archivos recibidos
        tasks = [
            process_one_file(
                uploaded_file=uf,
                insurance_company_name=insurance_company_name,
                semaphore=semaphore,
            )
            for uf in excel_files
        ]
        results = await asyncio.gather(*tasks)

        # 5) Concatenación de dataframes de cada resultado
        # Filtrar resultados válidos (en caso de que alguno falle) y extraer las llaves
        census_templates = []
        sinisters_templates = []
        summary_tables: list[pd.DataFrame] = []
        for res in results:
            if res and "reasoning_table" in res:
                summary_tables.append(res["reasoning_table"])
            if res and "census_template" in res:
                census_templates.append(res["census_template"])
            if res and "sinisters_template" in res:
                sinisters_templates.append(res["sinisters_template"])

        if not census_templates or not sinisters_templates:
            logger.error(
                "No se pudieron generar templates a partir de los archivos proporcionados."
            )
            raise HTTPException(
                status_code=500,
                detail="Internal server error during data extraction and parsing.",
            )

        # Concatenar todos los DataFrames
        complete_summary_tables = pd.concat(
            summary_tables, axis=0, join="outer", ignore_index=True
        )
        complete_census_template = pd.concat(census_templates, ignore_index=True)
        complete_sinisters_template = pd.concat(sinisters_templates, ignore_index=True)

        # 3) Descargar: crear Excel final con las dos hojas
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            # NOTE: ¡IMPORTANTE! Devolvemos la tabla de descuentos como parte del resultado
            payment_type_insurance_discounts_df = pd.DataFrame(
                data=PAYMENT_TYPE_INSURANCE_DISCOUNTS_DICT
            )
            payment_type_insurance_discounts_df = (
                payment_type_insurance_discounts_df.reset_index(drop=False).rename(
                    columns={"index": "Aseguradora"}
                )
            )
            payment_type_insurance_discounts_df.to_excel(
                writer,
                sheet_name="Tabla de descuentos",
                index=False,
            )
            complete_summary_tables.to_excel(
                writer,
                sheet_name="Resumen",
                index=False,
            )
            complete_census_template.to_excel(
                writer,
                sheet_name="Census template",
                index=False,
            )
            complete_sinisters_template.to_excel(
                writer,
                sheet_name="Sinisters template",
                index=False,
            )

        # Reposicionar el puntero del BytesIO
        output.seek(0)

        total_end = time.time()
        logger.info(
            "FOLIO %s PROCESADO EN %s SEGUNDOS",
            folio_id,
            "{:,.2f}".format(total_end - total_start),
        )

        # Return the Excel final como respuesta
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": 'attachment; filename="{folio_id} results.xlsx"'.format(
                    folio_id=folio_id
                )
            },
        )
    except Exception as e:
        logger.exception("Error in Extracting and Validating Data: %s", e)
        raise HTTPException(
            status_code=500,
            detail="Internal server error during data extraction and parsing.",
        ) from e


@app.get("/")
async def root():
    return {
        "message": "Welcome to the CENSUS AND SINISTERS Data Gathering and Parsing API!"
    }
