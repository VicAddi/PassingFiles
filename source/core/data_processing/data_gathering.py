import asyncio
import json
from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd
from num2words import num2words

from source.utils.logger import get_logger

logger = get_logger()


def extract_subtables_with_coords_from_excel_file_sheet_df(
    excel_file_name: str,
    excel_file_sheet_name: str,
    excel_file_sheet_tag: str,
    excel_file_sheet_df: pd.DataFrame,
    connectivity: int = 4,
    first_row_as_header: bool = True,
) -> list[dict[str, str | pd.DataFrame | tuple[int, int]]]:
    """
    Detecta subtables (sub-tablas) dentro de una DataFrame que proviene de una hoja de Excel
    con múltiples tablas separadas por filas/columnas en blanco.

    - Cada subtabla corresponde a un componente conectado de celdas no vacías (NaN): usa
      conectividad 4 (arriba/abajo/izq/der) por defecto, o 8 si connectivity=8.

    - Opcionalmente, asigna como headers el primer renglón de la subtabla y elimina ese
      renglón del cuerpo de datos.

    - Devuelve una lista de dicts. Cada dict contiene:
        - 'subtable': el DataFrame de la subtabla
        - 'coords': lista de coordenadas originales [(row, col), ...] que pertenecen a esa subtabla
        - 'rows': lista de filas originales que pertenecen a la subtabla
        - 'cols': lista de columnas originales que pertenecen a la subtabla
        - 'bounding_box': (min_row, max_row, min_col, max_col)

    Args:
      excel_file_name: nombre del archivo (solo para logs).
      excel_file_sheet_name: nombre de la hoja (solo para logs).
      excel_file_sheet_df: DataFrame de la hoja ya leído (con header=None si corresponde).
      connectivity: 4 o 8, define si las celdas conectadas diagonalmente también se cuentan.
      first_row_as_header: si True, usa la primera fila de la subtabla como headers y la elimina
                           del cuerpo de datos.

    Returns:
      List[Dict]: lista de subtables con sus coordenadas y metadatos.
    """
    if not isinstance(excel_file_sheet_df, pd.DataFrame):
        raise TypeError("excel_file_sheet_df debe ser un pandas DataFrame")

    if connectivity not in (4, 8):
        raise ValueError("connectivity debe ser 4 o 8")

    logger.info(
        "EXTRAYENDO TODAS LAS SUBTABLAS. ARCHIVO: %s. PESTAÑA: %s. ETIQUETA: %s",
        excel_file_name,
        excel_file_sheet_name,
        excel_file_sheet_tag,
    )
    # Obtenemos la matriz que representa la tabla del excel sheet
    # Esta matriz es un numpy array
    arr = excel_file_sheet_df.values
    # Obtenemos todas la celdas que NO SON "NaN" también como un Numpy Array
    non_empty = ~pd.isna(arr)
    nrows, ncols = non_empty.shape
    # Creamos una copia de la matriz que no es NaN y la llenamos de puros "True", "False"
    visited = np.zeros_like(non_empty, dtype=bool)

    if connectivity == 4:
        neighs = [(1, 0), (-1, 0), (0, 1), (0, -1)]
    elif connectivity == 8:
        neighs = [(1, 0), (-1, 0), (0, 1), (0, -1), (1, 1), (1, -1), (-1, 1), (-1, -1)]
    else:
        raise ValueError("'connectivity' param can only be 4 or 8.")

    comps_coords: list[list[tuple[int, int]]] = []

    # Descubrimos todos los componentes como listas de coords
    for i in range(nrows):
        for j in range(ncols):
            if non_empty[i, j] and not visited[i, j]:
                stack = [(i, j)]
                visited[i, j] = True
                coords = []
                while stack:
                    r, c = stack.pop()
                    coords.append((r, c))
                    for dr, dc in neighs:
                        nr, nc = r + dr, c + dc
                        if 0 <= nr < nrows and 0 <= nc < ncols:
                            if non_empty[nr, nc] and not visited[nr, nc]:
                                visited[nr, nc] = True
                                stack.append((nr, nc))
                comps_coords.append(coords)

    # Construimos cada subtabla y guardamos las coordenadas
    subtables_with_coords = []
    for coords in comps_coords:
        rows = sorted(set(r for r, _ in coords))
        cols = sorted(set(c for _, c in coords))
        row_pos = {r: idx for idx, r in enumerate(rows)}
        col_pos = {c: idx for idx, c in enumerate(cols)}

        data = np.full((len(rows), len(cols)), np.nan, dtype=object)
        for r, c in coords:
            data[row_pos[r], col_pos[c]] = excel_file_sheet_df.iat[r, c]

        # Opcional: asignar headers desde la primera fila de la subtabla
        if first_row_as_header and len(rows) > 0:
            header_row = rows[0]
            header_values = []
            for c in cols:
                val = excel_file_sheet_df.iat[header_row, c]
                if pd.isna(val):
                    header_values.append(f"col_{c}")
                else:
                    header_values.append(str(val))

            # Construimos la parte de datos sin la fila de header
            if len(rows) > 1:
                data_no_header = data[1:, :]
                sub = pd.DataFrame(
                    data_no_header, index=rows[1:], columns=header_values
                )
            else:
                # Solo había una fila; el cuerpo de datos queda vacío, pero ya hay headers
                data_no_header = np.empty((0, len(cols)), dtype=object)
                sub = pd.DataFrame(
                    data_no_header, index=rows[1:], columns=header_values
                )
        else:
            # Mantener el comportamiento anterior: sin usar la primera fila como header
            sub = pd.DataFrame(data, index=rows, columns=cols)

        subtables_with_coords.append(
            {
                "subtable": sub,
                "coords": coords,  # lista de (row, col) originales
                "rows": rows,  # filas originales usadas
                "cols": list(sub.columns),  # columnas originales usadas
                "bounding_box": (
                    rows[0],
                    rows[-1],
                    cols[0],
                    cols[-1],
                ),  # (min_row, max_row, min_col, max_col)
            }
        )

    logger.info(
        "SE EXTRAJERON %s SUBTABLAS. ARCHIVO: %s. PESTAÑA: %s. ETIQUETA: %s",
        f"{len(subtables_with_coords):,}",
        excel_file_name,
        excel_file_sheet_name,
        excel_file_sheet_tag,
    )
    dict_to_return = {
        "excel_file_name": excel_file_name,
        "excel_file_sheet_name": excel_file_sheet_name,
        "excel_file_sheet_tag": excel_file_sheet_tag,
        "excel_file_sheet_df": excel_file_sheet_df,
        "excel_file_sheet_extracted_dfs_results": subtables_with_coords,
    }
    return dict_to_return


async def extract_subtables_from_excel_async_to_thread(
    excel_file_name: str,
    excel_file_sheet_name: str,
    excel_file_sheet_tag: str,
    excel_file_sheet_df: pd.DataFrame,
    connectivity: int = 4,
    first_row_as_header: bool = True,
) -> list[dict[str, str | pd.DataFrame]]:
    """
    Versión asíncrona que corre la función sincrónica en un thread.
    No modifica la función original; solo ofrece una interfaz async.
    """
    return await asyncio.to_thread(
        extract_subtables_with_coords_from_excel_file_sheet_df,
        excel_file_name,
        excel_file_sheet_name,
        excel_file_sheet_tag,
        excel_file_sheet_df,
        connectivity,
        first_row_as_header,
    )


def group_and_concat_dfs_using_its_columns_names(
    excel_file_name: str,
    excel_file_sheet_name: str,
    excel_file_sheet_tag: str,
    excel_file_sheet_extracted_dfs_results: list[pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Agrupa una lista de DataFrames por tener exactamente el mismo conjunto de columnas
    (el orden de las columnas no importa) y concatena los DataFrames dentro de cada grupo.
    Devuelve un diccionario con llaves 'group_one', 'group_two', etc., en el orden de aparición
    de los grupos.

    Args:
        dfs: lista de DataFrames a agrupar.

    Returns:
        Diccionario con los grupos concatenados.
    """
    logger.info(
        "ORGANIZANDO TODAS LAS SUBTABLAS. ARCHIVO: %s PESTAÑA: %s ETIQUETA DE PESTAÑA: %s",
        excel_file_name,
        excel_file_sheet_name,
        excel_file_sheet_tag,
    )
    # Manejo rápido de caso vacío
    if not excel_file_sheet_extracted_dfs_results:
        return {}
    # Agrupar por el conjunto de columnas (sin importar el orden)
    groups_map: dict[frozenset, list[pd.DataFrame]] = {}
    for df in excel_file_sheet_extracted_dfs_results:
        key = frozenset(df.columns)
        groups_map.setdefault(key, []).append(df)

    # Construir resultado en el orden en que aparecieron los grupos
    result: dict[str, pd.DataFrame] = {}
    for i, (_cols, df_list) in enumerate(groups_map.items(), start=1):
        concatenated = pd.concat(df_list, ignore_index=True)
        concatenated = concatenated.reset_index(drop=True)
        result[f"group_{num2words(i)}"] = concatenated

    # Log de resultados
    logger.info(
        "¡ORGANIZACIÓN COMPLETADA! SE ENCONTRARON %s GRUPOS DE DATOS/TABLAS PARA ARCHIVO: %s PESTAÑA: %s. ETITQUETA DE PESTAÑA: %s",
        f"{len(result.keys()):,}",
        excel_file_name,
        excel_file_sheet_name,
        excel_file_sheet_tag,
    )
    dict_to_return = {
        "excel_file_name": excel_file_name,
        "excel_file_sheet_name": excel_file_sheet_name,
        "excel_file_sheet_tag": excel_file_sheet_tag,
        "excel_file_sheet_dfs_groups_dict": result,
    }
    return dict_to_return


async def group_and_concat_dfs_using_its_columns_names_async_to_thread(
    excel_file_name: str,
    excel_file_sheet_name: str,
    excel_file_sheet_tag: str,
    excel_file_sheet_extracted_dfs_results: list[pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    return await asyncio.to_thread(
        group_and_concat_dfs_using_its_columns_names,
        excel_file_name,
        excel_file_sheet_name,
        excel_file_sheet_tag,
        excel_file_sheet_extracted_dfs_results,
    )


def aggregate_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Agrupa una lista de diccionarios por las claves:
      'excel_file_name', 'excel_file_sheet_name',
      'excel_sheet_group_df_name', 'excel_sheet_group_df_tag'
    y devuelve una lista de diccionarios con las llaves:
      'excel_file_name', 'excel_file_sheet_name',
      'excel_sheet_group_df_name', 'excel_sheet_group_df_tag',
      'excel_sheet_group_df', 'fields_to_rename_on_dict'

    fields_to_rename_on_dict es un diccionario consolidado que mapea
    matched_column_name -> template_field_to_find_name extraído de
    finding_template_field_results de los elementos que pertenecen al grupo.
    """
    # Agrupar por las cuatro claves
    groups = defaultdict(list)
    for it in items:
        key = (
            it.get("excel_file_name"),
            it.get("excel_file_sheet_name"),
            it.get("excel_sheet_group_df_name"),
            it.get("excel_sheet_group_df_tag"),
        )
        groups[key].append(it)

    result = []
    for key, group_items in groups.items():
        (
            excel_file_name,
            excel_file_sheet_name,
            excel_sheet_group_df_name,
            excel_sheet_group_df_tag,
        ) = key

        # Tomamos el valor representativo de excel_sheet_group_df del primer item
        excel_sheet_group_df = group_items[0]["excel_sheet_group_df"]
        if excel_sheet_group_df is None:
            raise ValueError
        # Construimos fields_to_rename_on_dict combinando los valores de cada item del grupo
        fields_to_rename = {}
        for it in group_items:
            finding = it.get("finding_template_field_results", {})
            template_field = finding.get("template_field_to_find_name")
            matched = finding.get("matched_column_name")
            if template_field is not None and matched is not None:
                fields_to_rename[matched] = template_field

        result.append(
            {
                "excel_file_name": excel_file_name,
                "excel_file_sheet_name": excel_file_sheet_name,
                "excel_sheet_group_df_name": excel_sheet_group_df_name,
                "excel_sheet_group_df_tag": excel_sheet_group_df_tag,
                "excel_sheet_group_df": excel_sheet_group_df,
                "fields_to_rename_on_dict": fields_to_rename,
            }
        )

    return result
