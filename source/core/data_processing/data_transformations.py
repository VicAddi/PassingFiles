import asyncio
import time
import unicodedata
from typing import Optional, Set

import numpy as np
import pandas as pd

from ...config import (
    FINAL_CENSUS_TEMPLATE_COLUMN_NAMES,
    FINAL_SINISTER_TEMPLATE_COLUMN_NAMES,
    PAYMENT_TYPE_INSURANCE_DISCOUNTS_DICT,
)
from ...core.data_processing.data_analyzer import (
    pre_final_template_census_homologate_partnership_values_using_llm,
    pre_final_template_sinisters_pago_directo_homologate_values_using_llm,
)
from ...utils.logger import get_logger

logger = get_logger()


# region SINIESTRALIDAD
async def transform_pre_final_template_sinisters_df(
    excel_file_name: str,
    insurance_company_name: str,
    pre_final_template_sinisters_df: pd.DataFrame,
) -> pd.DataFrame:
    logger.info(
        "[SINIESTER TEMPLATE TRANSFORMATIONS] GENERANDO TEMPLATE FINAL PARA ARCHIVO %s APLICANDO TRANSFORMACIONES...",
        excel_file_name,
    )

    # 0) copiar el dataframe de entrada
    final_template_sinisters_df = pre_final_template_sinisters_df.copy()

    # 1) Mapear "Tipo de Pago" si existe la columna
    mapping_dict_pago_directo_field_siniestralidad_template = None
    if "Tipo de Pago" in pre_final_template_sinisters_df.columns:
        payment_type_s_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] HOMOLOGANDO TIPO DE PAGO PARA ARCHIVO: %s...",
            excel_file_name,
        )
        final_template_sinisters_df["Tipo de Pago"] = final_template_sinisters_df[
            "Tipo de Pago"
        ].fillna("Sin desglose")
        mapping_dict_pago_directo_field_siniestralidad_template = await pre_final_template_sinisters_pago_directo_homologate_values_using_llm(
            excel_file_name=excel_file_name,
            excel_file_pre_final_siniestralidad_template_df=pre_final_template_sinisters_df,
            pre_final_template_pago_directo_unique_values_list=list(
                pre_final_template_sinisters_df["Tipo de Pago"].dropna().unique()
            ),
        )
        final_template_sinisters_df["Tipo de Pago"] = final_template_sinisters_df[
            "Tipo de Pago"
        ].map(
            lambda x: (
                mapping_dict_pago_directo_field_siniestralidad_template[
                    "pago_directo_homologation_results"
                ]["grouping_dict"][x]
                if (
                    x
                    in mapping_dict_pago_directo_field_siniestralidad_template[
                        "pago_directo_homologation_results"
                    ]["grouping_dict"].keys()
                    and mapping_dict_pago_directo_field_siniestralidad_template[
                        "pago_directo_homologation_results"
                    ]["grouping_dict"][x]
                    != "Desconocido"
                )
                else x
            )
        )
        payment_type_e_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] TIPO DE PAGO HOMOLOGADO PARA ARCHIVO: %s... DURACIÓN: %s SEGUNDOS",
            excel_file_name,
            f"{payment_type_e_t - payment_type_s_t:,.2f}",
        )
    else:
        # Si no existe la columna, saltamos este paso
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] NO SE ENCONTRÓ LA COLUMNA TIPO DE PAGO PARA ARCHIVO: %s",
            excel_file_name,
        )

    # 2) Fecha de pago (solo si existe la columna)
    if "Fecha de Pago" in final_template_sinisters_df.columns:
        payment_date_s_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] PARSEANDO FECHA DE PAGO PARA ARCHIVO: %s...",
            excel_file_name,
        )

        # Ejecutamos este bloque en un thread para no bloquear el event loop
        def _process_fecha_pago(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df["Fecha de Pago"] = pd.to_datetime(df["Fecha de Pago"], errors="coerce")
            df["Fecha de Pago"] = df["Fecha de Pago"].dt.strftime("%d/%m/%Y")
            df["Fecha de Pago"] = df["Fecha de Pago"].where(
                df["Fecha de Pago"].notna(), ""
            )
            return df

        final_template_sinisters_df = await asyncio.to_thread(
            _process_fecha_pago, final_template_sinisters_df
        )
        payment_date_e_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] ¡FECHA DE PAGO PARA PARSEADA! ARCHIVO: %s... DURACIÓN: %s SEGUNDOS",
            excel_file_name,
            f"{payment_date_e_t - payment_date_s_t:,.2f}",
        )
    else:
        # Si no existe la columna, saltamos este paso
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] NO SE ENCONTRÓ LA FECHA DE PAGO PARA ARCHIVO: %s",
            excel_file_name,
        )
    # 3) ASEGURADORA (siempre asignamos, la columna se crea si no existe)
    final_template_sinisters_df["ASEGURADORA"] = insurance_company_name

    # 4) %descuento aplicado
    def _compute_descuento(row):
        tpg = row.get("Tipo de Pago")
        a = row.get("ASEGURADORA")
        if pd.notna(a) and pd.notna(tpg):
            return PAYMENT_TYPE_INSURANCE_DISCOUNTS_DICT.get(tpg, {}).get(a, np.nan)
        return np.nan

    if {"Tipo de Pago", "ASEGURADORA"}.issubset(final_template_sinisters_df.columns):
        percentage_discount_s_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] CALCULANDO PORCENTAJE DE DESCUENTO PARA ARCHIVO: %s...",
            excel_file_name,
        )
        # Solo intentar calcular si la estructura es válida
        final_template_sinisters_df["%descuento aplicado"] = (
            final_template_sinisters_df.apply(_compute_descuento, axis=1)
        )
        percentage_discount_e_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] PORCENTAJE DE DESCUENTO CALCULADO! ARCHIVO: %s... DURACIÓN: %s SEGUNDOS",
            excel_file_name,
            f"{percentage_discount_e_t - percentage_discount_s_t:,.2f}",
        )

    else:
        # Si no existe la columna, saltamos este paso
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] NO SE ENCONTRÓ LA COLUMNA 'Tipo de Pago' O 'ASEGURADORA' PARA ARCHIVO: %s POR LO TANTO NO SE CALCULA PORCENTAJE DE DESCUENTO",
            excel_file_name,
        )
        final_template_sinisters_df["%descuento aplicado"] = np.nan

    # 5) Pagos
    # Si existe "IVA Pagado" (según el comentario, LLM encontró IVA), procesamos esa ruta
    def _process_pagos_con_iva(df: pd.DataFrame) -> pd.DataFrame:
        payments_s_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] CALCULANDO 'Pagos' PARA ARCHIVO: %s...",
            excel_file_name,
        )
        df = df.copy()
        # Garantizar tipo numérico en la columna de descuento antes de cualquier aritmética
        if "%descuento aplicado" in df.columns:
            df["%descuento aplicado"] = pd.to_numeric(
                df["%descuento aplicado"], errors="coerce"
            )
        # Parseo de IVA Pagado
        if "IVA Pagado" in df.columns:
            logger.info(
                "[SINIESTERS TEMPLATE TRANSFORMATIONS] SE IDENTIFICÓ COLUMNA DE IVA EN LOS DATOS CALCULANDO 'Pagos' USANDO IVA. ARCHIVO: %s...",
                excel_file_name,
            )
            df["Monto Pagado Reportado"] = pd.to_numeric(
                df["Monto Pagado Reportado"], errors="coerce"
            )
            df["IVA Pagado"] = pd.to_numeric(
                df["IVA Pagado"], errors="coerce"
            )
            df["Pagos"] = (df["Monto Pagado Reportado"] + df["IVA Pagado"]) * (
                1 - df["%descuento aplicado"]
            )
            df = df.rename(columns={"IVA Pagado": "IVA"})
        else:
            if "Monto Pagado Reportado" in df.columns:
                df["Monto Pagado Reportado"] = pd.to_numeric(
                    df["Monto Pagado Reportado"], errors="coerce"
                )
                # NOTE: REGLA IMPORTANTE
                # Si no hay IVA Pagado se añade 10% al Monto Pagado Reportado
                df["Pagos"] = (df["Monto Pagado Reportado"] * (1 + 0.10)) * (
                    1 - df["%descuento aplicado"]
                )
            else:
                logger.info(
                    "[SINIESTERS TEMPLATE TRANSFORMATIONS] NO SE IDENTIFICÓ COLUMNA DE 'Monto Pagado Reportado'. ARCHIVO: %s...",
                    excel_file_name,
                )

                df["Pagos"] = np.nan
        payments_e_t = time.time()
        logger.info(
            "[SINIESTERS TEMPLATE TRANSFORMATIONS] ¡'Pagos' CALCULADO! ARCHIVO: %s... DURACIÓN: %s SEGUNDOS",
            excel_file_name,
            f"{payments_e_t- payments_s_t:,.2f}",
        )
        return df

    final_template_sinisters_df = await asyncio.to_thread(
        _process_pagos_con_iva, final_template_sinisters_df
    )

    # 6) Reordenar columnas y mapear al template final
    final_template_sinisters_df = final_template_sinisters_df.reindex(
        columns=FINAL_SINISTER_TEMPLATE_COLUMN_NAMES
    )
    # 7) Eliminamos todos los renglones que NO TIENEN ID DE SINIESTRO
    final_template_sinisters_df = final_template_sinisters_df.dropna(
        subset=["SINIESTRO"]
    )
    return final_template_sinisters_df


# endregion


# region CENSUS
async def harmonize_gender_column_async(
    df: pd.DataFrame,
    column_gender: str = "Género",
    male_variants: Optional[Set[str]] = None,
    female_variants: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """
    Devuelve una copia del DataFrame con la columna de género normalizada a 'M'/'F'.
    Se ejecuta en un executor para no bloquear el event loop.

    Parámetros:
    - df: DataFrame de entrada.
    - column_gender: nombre de la columna a normalizar (por defecto 'Género').
    - male_variants: conjunto de variantes que deben mapearse a 'M' (si None, se usan valores por defecto).
    - female_variants: conjunto de variantes que deben mapearse a 'F' (si None, se usan valores por defecto).

    Observaciones:
    - El resultado deja los valores no mapeados tal como están (incluyendo NaN).
    - El procesamiento es vectorizado y no modifica el resto del DataFrame.
    """
    if male_variants is None:
        male_variants = {"m", "masculino", "hombre", "h", "masc", "male"}
    if female_variants is None:
        female_variants = {"f", "femenino", "mujer", "fem", "female"}

    def _harmonize(
        df_in: pd.DataFrame, col: str, male_vars: Set[str], female_vars: Set[str]
    ) -> pd.DataFrame:
        df_out = df_in.copy()
        if col in df_out.columns:
            # Crear una copia de la columna original para conservar valores no mapeados
            result = df_out[col].copy()
            s = df_out[col].astype(str).str.strip().str.lower()
            s = s.apply(
                lambda x: unicodedata.normalize("NFKD", x)
                .encode("ascii", "ignore")
                .decode("ascii")
            )
            is_male = s.isin(male_vars)
            is_female = s.isin(female_vars)

            # Asignar 'M'/'F' solo a los lugares que coincidan. El resto conserva su valor original.
            result[is_male] = "M"
            result[is_female] = "F"
            df_out[col] = result
        return df_out

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None, _harmonize, df, column_gender, male_variants, female_variants
    )
    return result


async def transform_pre_final_template_census_df(
    excel_file_name: str,
    pre_final_template_census_df: pd.DataFrame,
) -> pd.DataFrame:
    logger.info(
        "[CENSUS TEMPLATE TRANSFORMATIONS] GENERANDO TEMPLATE FINAL PARA ARCHIVO %s. APLICANDO TRANSFORMACIONES...",
        excel_file_name,
    )

    # 0) copiar el dataframe de entrada
    final_template_census_df = pre_final_template_census_df.copy()

    # 1) Cuando "Género" existe en el dataframe entonces procedemos a homologar
    if "Género" in final_template_census_df.columns:
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] HOMOLOGANDO COLUMNA 'Género'. ARCHIVO %s.",
            excel_file_name,
        )
        gender_homologation_s_t = time.time()
        final_template_census_df = await harmonize_gender_column_async(
            df=final_template_census_df
        )
        gender_homologation_e_t = time.time()
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] ¡COLUMNA 'Género' HOMOLOGADA!. ARCHIVO %s. DURACIÓN: %s segundos",
            excel_file_name,
            f"{gender_homologation_e_t - gender_homologation_s_t:,.2f}",
        )
    else:
        # Si no existe la columna, saltamos este paso
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] NO SE ENCONTRÓ LA COLUMNA 'Género'",
            excel_file_name,
        )
    # 2) Cuando 'Parentesco' existe entonces la procesamos
    if "Parentesco" in final_template_census_df.columns:
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] HOMOLOGANDO COLUMNA 'Parentesco'. ARCHIVO %s.",
            excel_file_name,
        )
        partnership_homologation_s_t = time.time()
        mapping_dict_parentesco_field_census_template = (
            await pre_final_template_census_homologate_partnership_values_using_llm(
                excel_file_name=excel_file_name,
                excel_file_pre_final_census_template_df=final_template_census_df,
                pre_final_template_partnership_values_unique_values_list=list(
                    final_template_census_df["Parentesco"].dropna().unique()
                ),
            )
        )
        final_template_census_df["Parentesco"] = final_template_census_df[
            "Parentesco"
        ].map(
            lambda x: (
                mapping_dict_parentesco_field_census_template[
                    "parentesco_homologation_results"
                ]["grouping_dict"][x]
                if (
                    x
                    in mapping_dict_parentesco_field_census_template[
                        "parentesco_homologation_results"
                    ]["grouping_dict"].keys()
                    and mapping_dict_parentesco_field_census_template[
                        "parentesco_homologation_results"
                    ]["grouping_dict"][x]
                    != "Desconocido"
                )
                else x
            )
        )
        partnership_homologation_e_t = time.time()
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] ¡COLUMNA 'Parentesco' HOMOLOGADA! ARCHIVO: %s... DURACIÓN: %s segundos",
            excel_file_name,
            f"{partnership_homologation_e_t - partnership_homologation_s_t:,.2f}",
        )
    else:
        # Si no existe la columna, saltamos este paso
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] NO SE ENCONTRÓ LA COLUMNA 'Parentesco'. ARCHIVO: %s",
            excel_file_name,
        )

    # 3) Cuando 'Fecha de nacimiento' existe entonces la procesamos
    if "Fecha de nacimiento" in final_template_census_df.columns:
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] FORMATEANDO COLUMNA 'Fecha de nacimiento'. ARCHIVO %s.",
            excel_file_name,
        )
        dob_s_t = time.time()
        final_template_census_df["Fecha de nacimiento"] = pd.to_datetime(
            final_template_census_df["Fecha de nacimiento"], errors="coerce"
        ).dt.strftime("%d/%m/%Y")
        dob_e_t = time.time()
        logger.info(
            "[CENSUS TEMPLATE TRANSFORMATIONS] ¡COLUMNA 'Fecha de nacimiento' FORMATEADA!. ARCHIVO %s. DURACIÓN: %s segundos",
            excel_file_name,
            f"{dob_e_t - dob_s_t:,.2f}",
        )

    # 4) Ahora reindexamos las columnas para generar el template final
    final_template_census_df = final_template_census_df.reindex(
        columns=FINAL_CENSUS_TEMPLATE_COLUMN_NAMES
    )
    return final_template_census_df


# endregion
