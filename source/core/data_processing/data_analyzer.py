import ast
import json
import time
from string import Template

import pandas as pd

from source.utils.logger import get_logger

from ...config import (
    FIND_TEMPLATE_FIELD_LLM_OUTPUT_DICT_SCHEMA,
    HOMOLOGATE_VALUES_USING_MAPPING_DICT_LLM_OUTPUT_DICT_SCHEMA,
    MAX_ROWS_FROM_DF_TO_MAP_EXCEL_FIELD_INT,
    MAX_ROWS_FROM_DF_TO_TAG_EXCEL_SHEET_GROUP_INT,
    MAX_ROWS_FROM_DF_TO_TAG_EXCEL_SHEET_INT,
    TAGGING_EXCEL_SHEET_DF_LLM_OUTPUT_DICT_SCHEMA,
)
from ...core.data_processing.data_parsing import (
    extract_json_string,
    parse_llm_response_as_python_dict,
)
from ...core.prompts.prompts import (
    FIND_TEMPLATE_FIELD_ON_DATAFRAME_PROMPT,
    MAP_PARTNERSHIP_COLUMN_VALUES_PROMPT,
    MAP_TIPO_PAGO_COLUMN_VALUES_PROMPT,
    TAG_EXCEL_SHEET_DATAFRAME_PROMPT,
)
from ...core.secure_gpt_calls.llm_call import execute_call_to_secure_gpt_llm

logger = get_logger()


async def tag_excel_sheet_dataframe_using_secure_gpt(
    excel_file_name: str,
    excel_sheet_name: str,
    excel_sheet_df: pd.DataFrame,
    tagging_data_prompt: str = TAG_EXCEL_SHEET_DATAFRAME_PROMPT,
    expected_response_dict_schema: dict = TAGGING_EXCEL_SHEET_DF_LLM_OUTPUT_DICT_SCHEMA,
) -> dict[str, str]:
    logger.info(
        "ETIQUETANDO DATOS DE ARCHIVO: %s NOMBRE DE LA PESTAÑA: %s",
        excel_file_name,
        excel_sheet_name,
    )

    s_t = time.time()
    # NOTE: Cuando DF empty, entonces no hace falta ejecutar el LLM
    if excel_sheet_df.empty:
        logger.info(
            "DATOS VACÍOS CORRESPONDEN A 'SIN ETIQUETA'. ARCHIVO: %s NOMBRE DE LA SHEET: %s",
            excel_file_name,
            excel_sheet_name,
        )
        execution_status = "SUCCESS"
        the_llm_response_as_dict = {
            "reasoning_process": "DATOS VACÍOS CORRESPONDEN A 'SIN ETIQUETA'",
            "dataset_tag": "SIN ETIQUETA",
        }
    else:
        # Usamos las primeras líneas del dataframe para que el modelo los lea
        tabular_data_markdown_str = excel_sheet_df.iloc[
            :MAX_ROWS_FROM_DF_TO_TAG_EXCEL_SHEET_INT, :
        ].to_markdown(index=False)
        # Aumentar el prompt
        the_augmented_system_prompt = Template(tagging_data_prompt).substitute(
            tabular_data_markdown_str=tabular_data_markdown_str
        )
        # LLAMADA AL LLM
        secure_gpt_azure_openai_llm_completion_dict = (
            await execute_call_to_secure_gpt_llm(
                system_prompt=the_augmented_system_prompt
            )
        )
        e_t = time.time()
        logger.info(
            "LLAMADA DEL LLM RECIBIDA. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s. DURACIÓN %s SEGUNDOS",
            excel_file_name,
            excel_sheet_name,
            f"{e_t - s_t:,.2f}",
        )
        parsing_s_t = time.time()
        logger.info(
            "PARSEANDO RESPUESTA DEL LLM. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s.",
            excel_file_name,
            excel_sheet_name,
        )
        parsed_llm_response = await parse_llm_response_as_python_dict(
            llm_response=secure_gpt_azure_openai_llm_completion_dict["choices"][0][
                "message"
            ]["content"],
            expected_response_dict_schema=expected_response_dict_schema,
        )
        execution_status = parsed_llm_response["execution_status"]
        the_llm_response_as_dict = parsed_llm_response["the_llm_response_as_dict"]
        parsing_e_t = time.time()
        logger.info(
            "¡RESPUESTA DEL LLM PARSEADA!. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s. DURACIÓN %s SEGUNDOS",
            excel_file_name,
            excel_sheet_name,
            f"{parsing_e_t - parsing_s_t:,.2f}",
        )

    # Construcción del resultado final
    the_final_response_as_dict = {
        "execution_status": execution_status,
        "excel_file_name": excel_file_name,
        "excel_file_sheet_name": excel_sheet_name,
        "tagging_results": the_llm_response_as_dict,
    }

    return the_final_response_as_dict


async def tag_excel_sheet_dataframe_group_using_secure_gpt(
    excel_file_name: str,
    excel_sheet_name: str,
    excel_sheet_df_group_name: str,
    excel_sheet_df_group: pd.DataFrame,
    tagging_data_prompt: str = TAG_EXCEL_SHEET_DATAFRAME_PROMPT,
    expected_response_dict_schema: dict = TAGGING_EXCEL_SHEET_DF_LLM_OUTPUT_DICT_SCHEMA,
) -> dict[str, str]:
    logger.info(
        "ETIQUETANDO DATOS DE ARCHIVO: %s NOMBRE DE LA PESTAÑA: %s GRUPO DE LA PESTAÑA: %s",
        excel_file_name,
        excel_sheet_name,
        excel_sheet_df_group_name,
    )

    if excel_sheet_df_group.empty:
        logger.info(
            "DATOS VACÍOS CORRESPONDEN A 'SIN ETIQUETA'. ARCHIVO: %s PESTAÑA: %s GRUPO DE PESTAÑA: %s",
            excel_file_name,
            excel_sheet_name,
            excel_sheet_df_group_name,
        )
        execution_status = "SUCCESS"
        the_llm_response_as_dict = {
            "reasoning_process": "DATOS VACÍOS CORRESPONDEN A 'SIN ETIQUETA'",
            "dataset_tag": "SIN ETIQUETA",
        }

    else:
        # Usamos las primeras 50 líneas del dataframe para que el modelo los lea
        tabular_data_markdown_str = excel_sheet_df_group.iloc[
            :MAX_ROWS_FROM_DF_TO_TAG_EXCEL_SHEET_GROUP_INT, :
        ].to_markdown(index=False)
        # Aumentar el prompt
        the_augmented_system_prompt = Template(tagging_data_prompt).substitute(
            tabular_data_markdown_str=tabular_data_markdown_str
        )
        # LLAMADA AL LLM
        s_t = time.time()
        secure_gpt_azure_openai_llm_completion_dict = (
            await execute_call_to_secure_gpt_llm(
                system_prompt=the_augmented_system_prompt
            )
        )
        e_t = time.time()
        logger.info(
            "LLAMADA DEL LLM RECIBIDA. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s GRUPO DE PESTAÑA %s. DURACIÓN %s SEGUNDOS",
            excel_file_name,
            excel_sheet_name,
            excel_sheet_df_group_name,
            f"{e_t - s_t:,.2f}",
        )
        parsing_s_t = time.time()
        logger.info(
            "PARSEANDO RESPUESTA DEL LLM. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s.",
            excel_file_name,
            excel_sheet_name,
        )
        parsed_llm_response = await parse_llm_response_as_python_dict(
            llm_response=secure_gpt_azure_openai_llm_completion_dict["choices"][0][
                "message"
            ]["content"],
            expected_response_dict_schema=expected_response_dict_schema,
        )
        execution_status = parsed_llm_response["execution_status"]
        the_llm_response_as_dict = parsed_llm_response["the_llm_response_as_dict"]
        parsing_e_t = time.time()
        logger.info(
            "¡RESPUESTA DEL LLM PARSEADA!. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s. DURACIÓN %s SEGUNDOS",
            excel_file_name,
            excel_sheet_name,
            f"{parsing_e_t - parsing_s_t:,.2f}",
        )

    the_final_response_as_dict = {
        "execution_status": execution_status,
        "excel_file_name": excel_file_name,
        "excel_file_sheet_name": excel_sheet_name,
        "group_df_tagging_results": {
            "group_name": excel_sheet_df_group_name,
            "group_df": excel_sheet_df_group,
            **the_llm_response_as_dict,
        },
    }
    return the_final_response_as_dict


async def find_template_field_on_excel_sheet_group_df(
    excel_file_name: str,
    excel_sheet_name: str,
    excel_sheet_group_df_name: str,
    excel_sheet_group_df_tag: str,
    excel_sheet_group_df: pd.DataFrame,
    template_field_to_find_name: str,
    template_field_to_find_description: str,
    find_template_field_excel_sheet_group_df_prompt: str = FIND_TEMPLATE_FIELD_ON_DATAFRAME_PROMPT,
    expected_response_dict_schema: dict = FIND_TEMPLATE_FIELD_LLM_OUTPUT_DICT_SCHEMA,
) -> dict[str, str]:
    logger.info(
        "ENCONTRANDO CAMPO %s EN ARCHIVO: %s NOMBRE DE LA PESTAÑA: %s GRUPO DE LA PESTAÑA: %s ETIQUETA DE GRUPO: %s",
        template_field_to_find_name,
        excel_file_name,
        excel_sheet_name,
        excel_sheet_group_df_name,
        excel_sheet_group_df_tag,
    )

    if excel_sheet_group_df.empty:
        logger.info(
            "DATOS VACÍOS CORRESPONDEN A 'SIN COLUMNA ENCONTRADA'. CAMPO %s EN ARCHIVO: %s NOMBRE DE LA PESTAÑA: %s GRUPO DE LA PESTAÑA: %s ETIQUETA DE GRUPO: %s",
            template_field_to_find_name,
            excel_file_name,
            excel_sheet_name,
            excel_sheet_group_df_name,
            excel_sheet_group_df_tag,
        )
        execution_status = "SUCCESS"
        the_llm_response_as_dict = {
            "reasoning_process": "DATOS VACÍOS CORRESPONDEN A 'SIN COLUMNA ENCONTRADA'",
            "matched_column_name": "SIN COLUMNA ENCONTRADA",
        }
    else:
        # Usamos las primeras n líneas del dataframe para que el modelo los lea
        tabular_data_markdown_str = excel_sheet_group_df.iloc[
            :MAX_ROWS_FROM_DF_TO_MAP_EXCEL_FIELD_INT, :
        ].to_markdown(index=False)
        # Aumentar el prompt
        the_augmented_system_prompt = Template(
            find_template_field_excel_sheet_group_df_prompt
        ).substitute(
            tabular_data_markdown_str=tabular_data_markdown_str,
            field_title=template_field_to_find_name,
            field_description=template_field_to_find_description,
        )
        # LLAMADA AL LLM
        s_t = time.time()
        secure_gpt_azure_openai_llm_completion_dict = (
            await execute_call_to_secure_gpt_llm(
                system_prompt=the_augmented_system_prompt
            )
        )
        e_t = time.time()
        logger.info(
            "LLAMADA DEL LLM RECIBIDA. CAMPO: %s. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s GRUPO DE PESTAÑA %s. ETIQUETA DE PESTAÑA: %s. DURACIÓN %s SEGUNDOS",
            template_field_to_find_name,
            excel_file_name,
            excel_sheet_name,
            excel_sheet_group_df_name,
            excel_sheet_group_df_tag,
            f"{e_t - s_t:,.2f}",
        )
        parsing_s_t = time.time()
        logger.info(
            "PARSEANDO RESPUESTA DEL LLM. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s.",
            excel_file_name,
            excel_sheet_name,
        )
        parsed_llm_response = await parse_llm_response_as_python_dict(
            llm_response=secure_gpt_azure_openai_llm_completion_dict["choices"][0][
                "message"
            ]["content"],
            expected_response_dict_schema=expected_response_dict_schema,
        )
        execution_status = parsed_llm_response["execution_status"]
        the_llm_response_as_dict = parsed_llm_response["the_llm_response_as_dict"]
        parsing_e_t = time.time()
        logger.info(
            "¡RESPUESTA DEL LLM PARSEADA!. NOMBRE DE ARCHIVO: %s. NOMBRE DE PESTAÑA: %s. DURACIÓN %s SEGUNDOS",
            excel_file_name,
            excel_sheet_name,
            f"{parsing_e_t - parsing_s_t:,.2f}",
        )

    the_final_response_as_dict = {
        "execution_status": execution_status,
        "excel_file_name": excel_file_name,
        "excel_file_sheet_name": excel_sheet_name,
        "excel_sheet_group_df_name": excel_sheet_group_df_name,
        "excel_sheet_group_df_tag": excel_sheet_group_df_tag,
        "excel_sheet_group_df": excel_sheet_group_df,
        "finding_template_field_results": {
            **the_llm_response_as_dict,
            "template_field_to_find_name": template_field_to_find_name,
        },
    }

    return the_final_response_as_dict


async def pre_final_template_census_homologate_partnership_values_using_llm(
    excel_file_name: str,
    excel_file_pre_final_census_template_df: pd.DataFrame,
    pre_final_template_partnership_values_unique_values_list: list[str],
    partnership_values_homologation_prompt: str = MAP_PARTNERSHIP_COLUMN_VALUES_PROMPT,
    expected_response_dict_schema: dict = HOMOLOGATE_VALUES_USING_MAPPING_DICT_LLM_OUTPUT_DICT_SCHEMA,
) -> dict[str, str | pd.DataFrame]:

    s_t = time.time()

    logger.info(
        "[TEMPLATE DE CENSUS] HOMOLOGANDO PARENTESCO PARA ARCHIVO: %s",
        excel_file_name,
    )
    # Aumentar el prompt
    the_augmented_system_prompt = Template(
        partnership_values_homologation_prompt
    ).substitute(
        parentesco_column_unique_values=json.dumps(
            pre_final_template_partnership_values_unique_values_list,
            indent=2,
            ensure_ascii=False,
        ),
    )
    # LLAMADA AL LLM
    # instanciar el cliente
    logger.info(
        "[TEMPLATE DE CENSUS] EJECUTANDO LLAMADA AL LLM PARA HOMOLOGAR PARENTESCO PARA ARCHIVO: %s",
        excel_file_name,
    )
    secure_gpt_azure_openai_llm_completion_dict = await execute_call_to_secure_gpt_llm(
        system_prompt=the_augmented_system_prompt,
    )
    e_t = time.time()
    logger.info(
        "[TEMPLATE DE CENSUS] LLAMADA DEL LLM RECIBIDA ARCHIVO %s. DURACIÓN %f SEGUNDOS",
        excel_file_name,
        e_t - s_t,
    )
    parsing_s_t = time.time()
    logger.info(
        "[TEMPLATE DE CENSUS] HOMOLOGANDO PARENTESCO. PARSEANDO RESPUESTA DEL LLM. NOMBRE DE ARCHIVO: %s.",
        excel_file_name,
    )
    parsed_llm_response = await parse_llm_response_as_python_dict(
        llm_response=secure_gpt_azure_openai_llm_completion_dict["choices"][0][
            "message"
        ]["content"],
        expected_response_dict_schema=expected_response_dict_schema,
    )
    execution_status = parsed_llm_response["execution_status"]
    the_llm_response_as_dict = parsed_llm_response["the_llm_response_as_dict"]
    parsing_e_t = time.time()
    logger.info(
        "[TEMPLATE DE CENSUS] HOMOLOGANDO PARENTESCO. ¡RESPUESTA DEL LLM PARSEADA!. NOMBRE DE ARCHIVO: %s. DURACIÓN %s SEGUNDOS",
        excel_file_name,
        f"{parsing_e_t - parsing_s_t:,.2f}",
    )

    the_final_response_as_dict = {
        "execution_status": execution_status,
        "excel_file_name": excel_file_name,
        "excel_file_template_tag": "CENSUS",
        "excel_file_template_df": excel_file_pre_final_census_template_df,
        "parentesco_homologation_results": the_llm_response_as_dict,
    }
    return the_final_response_as_dict


async def pre_final_template_sinisters_pago_directo_homologate_values_using_llm(
    excel_file_name: str,
    excel_file_pre_final_siniestralidad_template_df: pd.DataFrame,
    pre_final_template_pago_directo_unique_values_list: list[str],
    pago_directo_homologation_prompt: str = MAP_TIPO_PAGO_COLUMN_VALUES_PROMPT,
    expected_response_dict_schema: dict = HOMOLOGATE_VALUES_USING_MAPPING_DICT_LLM_OUTPUT_DICT_SCHEMA,
) -> dict[str, str]:

    s_t = time.time()

    logger.info(
        "[TEMPLATE DE SINIESTRALIDAD] HOMOLOGANDO TIPO DE PAGO PARA ARCHIVO: %s",
        excel_file_name,
    )
    # Aumentar el prompt
    the_augmented_system_prompt = Template(pago_directo_homologation_prompt).substitute(
        pago_directo_column_unique_values=json.dumps(
            pre_final_template_pago_directo_unique_values_list,
            indent=2,
            ensure_ascii=False,
        ),
    )
    # LLAMADA AL LLM
    s_t = time.time()
    secure_gpt_azure_openai_llm_completion_dict = await execute_call_to_secure_gpt_llm(
        system_prompt=the_augmented_system_prompt
    )
    e_t = time.time()
    logger.info(
        "[TEMPLATE DE SINIESTRALIDAD] HOMOLOGANDO TIPO DE PAGO. LLAMADA DEL LLM RECIBIDA. ARCHIVO: %s. DURACIÓN: %s",
        excel_file_name,
        f"{e_t - s_t:,.2f}",
    )
    parsing_s_t = time.time()
    logger.info(
        "[TEMPLATE DE SINIESTRALIDAD] HOMOLOGANDO TIPO DE PAGO. PARSEANDO RESPUESTA DEL LLM. NOMBRE DE ARCHIVO: %s.",
        excel_file_name,
    )
    parsed_llm_response = await parse_llm_response_as_python_dict(
        llm_response=secure_gpt_azure_openai_llm_completion_dict["choices"][0][
            "message"
        ]["content"],
        expected_response_dict_schema=expected_response_dict_schema,
    )
    execution_status = parsed_llm_response["execution_status"]
    the_llm_response_as_dict = parsed_llm_response["the_llm_response_as_dict"]
    parsing_e_t = time.time()
    logger.info(
        "[TEMPLATE DE SINIESTRALIDAD] HOMOLOGANDO TIPO DE PAGO. ¡RESPUESTA DEL LLM PARSEADA!. NOMBRE DE ARCHIVO: %s. DURACIÓN %s SEGUNDOS",
        excel_file_name,
        f"{parsing_e_t - parsing_s_t:,.2f}",
    )
    the_final_response_as_dict = {
        "execution_status": execution_status,
        "excel_file_name": excel_file_name,
        "excel_file_template_tag": "SINIESTRALIDAD",
        "excel_file_template_df": excel_file_pre_final_siniestralidad_template_df,
        "pago_directo_homologation_results": the_llm_response_as_dict,
    }
    return the_final_response_as_dict
