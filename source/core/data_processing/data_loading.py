import io
import json
import time

import pandas as pd

from ...utils.logger import get_logger

logger = get_logger()


async def parse_excel_bytes_to_dataframes(
    file_name: str,
    file_bytes: bytes,
) -> dict[str, str | dict[str, pd.DataFrame]]:
    """
    Parsea los bytes de un archivo Excel (XLS/XLSX) en un diccionario de DataFrames por cada sheet.

    Args:
        file_name (str): Nombre del archivo (incluyendo extensión).
        file_bytes (bytes): Contenido del archivo en bytes.
    Returns:
        dict:
            - "file_name": str, nombre del archivo.
            - "file_dataframes_from_sheets": dict[str, pandas.DataFrame], mapping de nombre de sheet -> DataFrame.

    Raises:
        ValueError: si no se puede leer el contenido como Excel.
    """
    s_t = time.time()
    logger.info(
        "EXTRAYENDO DATAFRAMES DE TODAS LAS HOJAS DEL EXCEL. ARCHIVO: %s", file_name
    )
    if not isinstance(file_bytes, (bytes, bytearray)): #pragma: no cover
        raise ValueError("file_bytes debe ser bytes")

    try:
        # Leer todos los sheets y devolver un dict {sheet_name: DataFrame}
        excel_sheets: dict[str, pd.DataFrame] = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=None,
            header=None,  # NOTE: Header None es para evitar que la primer línea del excel se lea como headers
        )
        logger.info(
            "SE HAN EXTRAÍDO %s DATAFRAMES DESDE EL ARCHIVO %s\nSHEET NAMES & DF SHAPES:\n%s",
            f"{len(list(excel_sheets.keys())):,}",
            file_name,
            json.dumps(
                {_key: _value.shape for _key, _value in excel_sheets.items()},
                indent=2,
                ensure_ascii=False,
            ),
        )
    except Exception as ex: # pragma: no cover
        raise ValueError(
            "Error al parsear bytes como Excel. Verifique que el contenido sea un archivo Excel válido."
        ) from ex
    e_t = time.time()
    logger.info(
        "¡DATAFRAMES EXTRAÍDOS DE TODAS LAS HOJAS DEL EXCEL. ARCHIVO: %s! DURACIÓN: %s segundos",
        file_name,
        f"{e_t - s_t:,.2f}",
    )
    return {
        "file_name": file_name,
        "file_dataframes_from_sheets": excel_sheets,
    }
