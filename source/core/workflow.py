import asyncio
import copy
import json
import time

import pandas as pd

from ..config import (
    FINAL_CENSUS_TEMPLATE_COLUMN_NAMES,
    FINAL_SINISTER_TEMPLATE_COLUMN_NAMES,
)
from ..core.data_processing.data_analyzer import (
    find_template_field_on_excel_sheet_group_df,
    tag_excel_sheet_dataframe_group_using_secure_gpt,
    tag_excel_sheet_dataframe_using_secure_gpt,
)
from ..core.data_processing.data_gathering import (
    aggregate_groups,
    extract_subtables_from_excel_async_to_thread,
    group_and_concat_dfs_using_its_columns_names_async_to_thread,
)
from ..core.data_processing.data_loading import parse_excel_bytes_to_dataframes
from ..core.data_processing.data_transformations import (
    transform_pre_final_template_census_df,
    transform_pre_final_template_sinisters_df,
)
from ..core.prompts.prompts import (
    CENSUS_FIELDS_DESCRIPTIONS_TO_FIND_DICT,
    SINIESTRALIDAD_FIELDS_DESCRIPTIONS_TO_FIND_DICT,
)
from ..utils.logger import get_logger

logger = get_logger()


async def gather_parse_and_structure_census_and_sinisters_templates_data_from_an_excel_file(
    excel_file_name: str,
    insurance_company_name: str,
    excel_file_bytes: bytes,
    excel_file_mime_type: str,
) -> pd.DataFrame:

    s_t = time.time()

    logger.info(
        "PROCESANDO ARCHIVO %s PARA RECOLECCIÓN Y CONSTRUCCIÓN DE TEMPLATES 'census' y 'siniestralidad'. MYME TYPE: %s",
        excel_file_name,
        excel_file_mime_type,
    )

    # 1.- EXTRACCIÓN/LECTURA. Leemos cada sheet del Excel como un dataframe
    excel_file_dataframes_dict = await parse_excel_bytes_to_dataframes(
        file_name=excel_file_name, file_bytes=excel_file_bytes
    )

    # 2.- INTERPRETACIÓN. Debemos seleccionar las pestañas que sí nos ayudan a llenar los templates.
    # Para ello, interpretamos su contenido y las etiquetamos
    # Para cada diccionario definimos una coroutine
    tagging_excel_sheet_df_coroutines = [
        tag_excel_sheet_dataframe_using_secure_gpt(
            excel_file_name=excel_file_dataframes_dict["file_name"],
            excel_sheet_name=_df_name,
            excel_sheet_df=excel_file_dataframes_dict["file_dataframes_from_sheets"][
                _df_name
            ].copy(),
        )
        for _df_name in excel_file_dataframes_dict["file_dataframes_from_sheets"].keys()
    ]
    # Ejecutamos las coroutinas
    tagging_excel_sheet_df_results = await asyncio.gather(
        *tagging_excel_sheet_df_coroutines, return_exceptions=False
    )

    # 3.- RECOLECCIÓN. La premisa es que cada una de las pestañas del excel tiene más de una tabla.
    # Así que recolectamos y agrupamos todas las tablas en función de la coincidencia en los nombres de sus columnas
    reasoning_tables_df_list: list[pd.DataFrame] = []
    tagging_excel_sheet_df_results_to_df = [
        {
            **{
                _key: _value
                for _key, _value in _resulting_dict.items()
                if _key not in ["tagging_results"]
            },
            **_resulting_dict["tagging_results"],
        }
        for _resulting_dict in tagging_excel_sheet_df_results
    ]
    tagging_excel_sheet_df_results_df = pd.DataFrame(
        data=tagging_excel_sheet_df_results_to_df
    )
    reasoning_tables_df_list.append(tagging_excel_sheet_df_results_df)
    logger.info(
        "FILE: %s TABLA DE RESULTADOS DE TAGGEO DE PESTAÑAS COMPUTADA dimensión -> (%s, %s)",
        excel_file_name,
        f"{tagging_excel_sheet_df_results_df.shape[0]:,}",
        f"{tagging_excel_sheet_df_results_df.shape[-1]:,}",
    )

    # 3.1 -  Usando los resultados de tageo y los dataframes, procesamos para extraer todas las tablas
    dicts_to_get_all_sheet_tables = []
    for _dict in tagging_excel_sheet_df_results:
        _aux_dict = copy.deepcopy(_dict)
        _aux_dict["sheet_df"] = excel_file_dataframes_dict[
            "file_dataframes_from_sheets"
        ][_aux_dict["excel_file_sheet_name"]].copy()
        dicts_to_get_all_sheet_tables.append(_aux_dict)

    # NOTE: Antes validamos que sí hay información por procesar
    all_dfs_to_fulfil_templates = [
        _sheet_dict
        for _sheet_dict in dicts_to_get_all_sheet_tables
        if _sheet_dict["tagging_results"]["dataset_tag"] in ["CENSUS", "SINIESTRALIDAD"]
    ]

    # NOTE: CUANDO NO HAY NINGÚN DF QUE AYUDE A LLENAR LOS TEMPLATE, ENTONCES DEVOLVEMOS DFS VACÍOS
    if len(all_dfs_to_fulfil_templates) == 0:
        logger.warning(
            "ARCHIVO EXCEL %s NO CONTIENE INFORMACIÓN PARA LLENAR LO TEMPLATES.",
            excel_file_name,
        )
        return {
            "excel_file_name": excel_file_name,
            "census_template": pd.DataFrame(),
            "sinisters_template": pd.DataFrame(),
        }
    else:
        # 3.2 - Corutinas
        # Definimos la lista de coroutines
        all_dfs_from_each_excel_sheet_coroutines = [
            extract_subtables_from_excel_async_to_thread(
                excel_file_name=_sheet_dict["excel_file_name"],
                excel_file_sheet_name=_sheet_dict["excel_file_sheet_name"],
                excel_file_sheet_tag=_sheet_dict["tagging_results"]["dataset_tag"],
                excel_file_sheet_df=_sheet_dict["sheet_df"].copy(),
            )
            for _sheet_dict in dicts_to_get_all_sheet_tables
            # NOTE: Solamente ejecutamos para las pestañas que sí ayudan a llenar los templates
            if _sheet_dict["tagging_results"]["dataset_tag"]
            in ["CENSUS", "SINIESTRALIDAD"]
        ]
        # 3.3 - Ejecución de coroutines
        all_dfs_from_each_excel_sheet_results = await asyncio.gather(
            *all_dfs_from_each_excel_sheet_coroutines, return_exceptions=False
        )
        # 3.4.- Recolección en función de la coincidencia de columnas
        # NOTE: La premisa es que cada pestaña tiene muchas tablas, pero dichas tablas pueden agruparse en función de sus columnas
        concatenated_dfs_coroutines = [
            group_and_concat_dfs_using_its_columns_names_async_to_thread(
                excel_file_name=_data_dict["excel_file_name"],
                excel_file_sheet_name=_data_dict["excel_file_sheet_name"],
                excel_file_sheet_tag=_data_dict["excel_file_sheet_tag"],
                excel_file_sheet_extracted_dfs_results=[
                    _dfs_dict["subtable"]
                    for _dfs_dict in _data_dict[
                        "excel_file_sheet_extracted_dfs_results"
                    ]
                ],
            )
            for _data_dict in all_dfs_from_each_excel_sheet_results
            # NOTE: Solamente ejecutamos para las pestañas que sí ayudan a llenar los templates
            if _data_dict["excel_file_sheet_tag"] in ["CENSUS", "SINIESTRALIDAD"]
        ]
        concatenated_dfs_results = await asyncio.gather(
            *concatenated_dfs_coroutines, return_exceptions=False
        )

        # 4.- Re interpretación
        # NOTE: A priori, las tablas encontradas en cada pestaña pueden ser o no de ayuda para llenar los template así que debemos 're filtrarlas'
        # COROUTINES
        tagging_excel_sheet_group_df_coroutines = []
        # Para cada sheet analizada
        for _sheet in concatenated_dfs_results:
            # Para cada grupo encontrado
            for _group in _sheet["excel_file_sheet_dfs_groups_dict"].keys():
                tagging_excel_sheet_group_df_coroutines.append(
                    tag_excel_sheet_dataframe_group_using_secure_gpt(
                        excel_file_name=_sheet["excel_file_name"],
                        excel_sheet_name=_sheet["excel_file_sheet_name"],
                        excel_sheet_df_group=_sheet["excel_file_sheet_dfs_groups_dict"][
                            _group
                        ],
                        excel_sheet_df_group_name=_group,
                    )
                )
        # EJECUCIÓN
        tagging_excel_sheet_group_df_results = await asyncio.gather(
            *tagging_excel_sheet_group_df_coroutines, return_exceptions=False
        )

        # Recolección para tabla de razonamiento
        tagging_excel_sheet_group_df_results_to_df = [
            {
                **{
                    _key: _value
                    for _key, _value in _resulting_dict.items()
                    if _key not in ["group_df_tagging_results"]
                },
                **{
                    _second_key: _second_value
                    for _second_key, _second_value in _resulting_dict[
                        "group_df_tagging_results"
                    ].items()
                    if _second_key not in ["group_df"]
                },
            }
            for _resulting_dict in tagging_excel_sheet_group_df_results
        ]

        # Generamos el DF
        tagging_excel_sheet_group_df_results_df = pd.DataFrame(
            data=tagging_excel_sheet_group_df_results_to_df
        )

        # Conjuntamos la tabla en la lista de tablas de razonamiento
        reasoning_tables_df_list.append(tagging_excel_sheet_group_df_results_df)

        logger.info(
            "FILE NAME: %s TABLA DE RESULTADOS DE TAGGEO DE GRUPOS PARA CADA PESTAÑA COMPUTADA. dimensión: (%s, %s)",
            excel_file_name,
            f"{tagging_excel_sheet_group_df_results_df.shape[0]:,}",
            f"{tagging_excel_sheet_group_df_results_df.shape[-1]:,}",
        )

        # FILTRAMOS SOLO AQUELLAS ÚTILES PARA LLENAR LOS TEMPLATE
        # Filtrar. Conservar solo los dataframes que nos ayudan a llenar los templates.
        filtered_by_useful_tagging_excel_sheet_group_df_results = [
            copy.deepcopy(_dict)
            for _dict in tagging_excel_sheet_group_df_results
            # NOTE: Solamente ejecutamos para las pestañas que sí ayudan a llenar los templates
            if _dict["group_df_tagging_results"]["dataset_tag"]
            in ["CENSUS", "SINIESTRALIDAD"]
        ]

        # 5.- ESTRUCTURACIÓN. La premisa es que los nombres de las columnas nunca son los mismos.
        # Así que debemos identificar y re nombrar las columnas en función de su descripción y los datos que las 'rellenan'.
        # Construimos todas las coroutinas
        all_template_name_finding_results_excel_sheet_group_df_coroutines = []
        for (
            tagged_group_df_data
        ) in filtered_by_useful_tagging_excel_sheet_group_df_results:
            if (
                tagged_group_df_data["group_df_tagging_results"]["dataset_tag"]
                == "CENSUS"
            ):
                # A las coroutines les pegamos todas las TASKS, hay una por cada campo que debemos encontrar
                census_corutines_for_every_template_field = [
                    find_template_field_on_excel_sheet_group_df(
                        excel_file_name=tagged_group_df_data["excel_file_name"],
                        excel_sheet_name=tagged_group_df_data["excel_file_sheet_name"],
                        excel_sheet_group_df_name=tagged_group_df_data[
                            "group_df_tagging_results"
                        ]["group_name"],
                        excel_sheet_group_df_tag=tagged_group_df_data[
                            "group_df_tagging_results"
                        ]["dataset_tag"],
                        excel_sheet_group_df=tagged_group_df_data[
                            "group_df_tagging_results"
                        ]["group_df"].copy(),
                        template_field_to_find_name=the_field,
                        template_field_to_find_description=its_description,
                    )
                    for the_field, its_description in CENSUS_FIELDS_DESCRIPTIONS_TO_FIND_DICT.items()
                ]
                all_template_name_finding_results_excel_sheet_group_df_coroutines.extend(
                    census_corutines_for_every_template_field
                )
            elif (
                tagged_group_df_data["group_df_tagging_results"]["dataset_tag"]
                == "SINIESTRALIDAD"
            ):
                # A las coroutines les pegamos todas las TASKS, hay una por cada campo que debemos encontrar
                siniestralidad_corutines_for_every_template_field = [
                    find_template_field_on_excel_sheet_group_df(
                        excel_file_name=tagged_group_df_data["excel_file_name"],
                        excel_sheet_name=tagged_group_df_data["excel_file_sheet_name"],
                        excel_sheet_group_df_name=tagged_group_df_data[
                            "group_df_tagging_results"
                        ]["group_name"],
                        excel_sheet_group_df_tag=tagged_group_df_data[
                            "group_df_tagging_results"
                        ]["dataset_tag"],
                        excel_sheet_group_df=tagged_group_df_data[
                            "group_df_tagging_results"
                        ]["group_df"].copy(),
                        template_field_to_find_name=the_field,
                        template_field_to_find_description=its_description,
                    )
                    for the_field, its_description in SINIESTRALIDAD_FIELDS_DESCRIPTIONS_TO_FIND_DICT.items()
                ]
                all_template_name_finding_results_excel_sheet_group_df_coroutines.extend(
                    siniestralidad_corutines_for_every_template_field
                )

        # Ejecutamos las coroutines
        all_template_name_finding_results_excel_sheet_group_df_results = (
            await asyncio.gather(
                *all_template_name_finding_results_excel_sheet_group_df_coroutines,
                return_exceptions=False,
            )
        )

        # Recolección para tabla de razonamiento
        all_template_name_finding_results_excel_sheet_group_df_results_to_df = [
            {
                **{
                    _key: _value
                    for _key, _value in _resulting_dict.items()
                    if _key
                    not in [
                        "tagging_results",
                        "excel_sheet_group_df",
                        "finding_template_field_results",
                    ]
                },
                **_resulting_dict["finding_template_field_results"],
            }
            for _resulting_dict in all_template_name_finding_results_excel_sheet_group_df_results
        ]

        # Generamos el DF
        template_name_finding_results_df = pd.DataFrame(
            data=all_template_name_finding_results_excel_sheet_group_df_results_to_df
        )

        # Conjuntamos la tabla en la lista de tablas de razonamiento
        reasoning_tables_df_list.append(template_name_finding_results_df)

        logger.info(
            "FILE NAME: %s TABLA DE RESULTADOS DE TAGGEO DE GRUPOS PARA CADA PESTAÑA COMPUTADA. dimensión: (%s, %s)",
            excel_file_name,
            f"{template_name_finding_results_df.shape[0]:,}",
            f"{template_name_finding_results_df.shape[-1]:,}",
        )

        # Con los resultados podemos renombrar las columnas de todos los grupos
        aggregated_results_from_finding_cols = aggregate_groups(
            all_template_name_finding_results_excel_sheet_group_df_results
        )
        # Renombramos
        renamed_results = []
        for _dict_to_rename in aggregated_results_from_finding_cols:
            _aux_dict = copy.deepcopy(_dict_to_rename)
            _aux_dict["excel_sheet_group_df"] = _aux_dict[
                "excel_sheet_group_df"
            ].rename(columns=_aux_dict["fields_to_rename_on_dict"])
            # Proceso de selección
            _aux_cols_to_select = [
                _column_name
                for _column_name in _aux_dict["fields_to_rename_on_dict"].values()
                if _column_name in _aux_dict["excel_sheet_group_df"]
            ]
            _aux_dict["excel_sheet_group_df"] = _aux_dict["excel_sheet_group_df"][
                _aux_cols_to_select
            ]
            _aux_dict["excel_sheet_group_df"]["excel_file_name"] = _aux_dict[
                "excel_file_name"
            ]
            _aux_dict["excel_sheet_group_df"]["excel_file_sheet_name"] = _aux_dict[
                "excel_file_sheet_name"
            ]
            renamed_results.append(_aux_dict)

        # Re agrupamos, pues hasta este punto ya tenemos los dos template 'en bruto'
        pre_final_tables_census: list[pd.DataFrame] = []
        pre_final_tables_siniestralidad: list[pd.DataFrame] = []
        for _pre_final_table_data in renamed_results:
            if _pre_final_table_data["excel_sheet_group_df_tag"] == "CENSUS":
                # Copiar el df
                _aux_df = _pre_final_table_data["excel_sheet_group_df"].copy()
                # Reordenar columnas y mapear al template final para que la concatenación no mate campos
                _aux_df = _aux_df.reindex(columns=FINAL_CENSUS_TEMPLATE_COLUMN_NAMES)
                pre_final_tables_census.append(_aux_df)
            elif _pre_final_table_data["excel_sheet_group_df_tag"] == "SINIESTRALIDAD":
                # Copiar el df
                _aux_df = _pre_final_table_data["excel_sheet_group_df"].copy()
                # Reordenar columnas y mapear al template final para que la concatenación no mate campos
                _aux_df = _aux_df.reindex(columns=FINAL_SINISTER_TEMPLATE_COLUMN_NAMES)
                pre_final_tables_siniestralidad.append(_aux_df)

        # NOTE: Cuando sí hay tablas para llenar el template de CENSUS entonces las concatenamos
        if pre_final_tables_census:
            pre_final_template_census = pd.concat(
                pre_final_tables_census,
                ignore_index=True,
                join="outer",
                axis="index",
                copy=True,
            )
            # Ahora aplicamos las transformaciones a cada uno de los templates
            # CENSUS
            census_final_template = await transform_pre_final_template_census_df(
                excel_file_name=excel_file_name,
                pre_final_template_census_df=pre_final_template_census.copy(),
            )

        else:
            census_final_template = pd.DataFrame()

        # NOTE: Cuando sí hay tablas para llenar el template de CENSUS entonces las concatenamos
        if pre_final_tables_siniestralidad:
            pre_final_template_siniestralidad = pd.concat(
                pre_final_tables_siniestralidad,
                ignore_index=True,
                join="outer",
                axis="index",
                copy=True,
            )
            # SINIESTRALIDAD
            sinisters_final_template = await transform_pre_final_template_sinisters_df(
                excel_file_name=excel_file_name,
                insurance_company_name=insurance_company_name,
                pre_final_template_sinisters_df=pre_final_template_siniestralidad.copy(),
            )
        else:
            sinisters_final_template = pd.DataFrame()

    e_t = time.time()
    logger.info(
        "¡ARCHIVO %s PARA RECOLECCIÓN Y CONSTRUCCIÓN DE 'TEMPLATES' PROCESADO CON ÉXITO! DURACIÓN: %s segundos.",
        excel_file_name,
        f"{e_t - s_t:,.2f}",
    )

    if reasoning_tables_df_list:
        reasoning_table = pd.concat(
            reasoning_tables_df_list, axis=0, join="outer", ignore_index=True
        )
    return {
        "excel_file_name": excel_file_name,
        "reasoning_table": reasoning_table,
        "census_template": census_final_template,
        "sinisters_template": sinisters_final_template,
    }
