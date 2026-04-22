import ast
import json
import re

from jsonschema import ValidationError, validate

from source.utils.logger import get_logger

logger = get_logger()


async def extract_json_string(text: str) -> dict[str, str] | None: # pragma: no cover
    """
    Intenta parsear JSON desde un texto. Si el texto contiene más que un JSON,
    intenta extraer el primer objeto JSON válido.
    """
    if not text or not isinstance(text, str):
        raise ValueError("'text' paramatere only can be type 'string'")

    # Intento directo
    try:
        return json.loads(text)
    except Exception:
        try:
            return ast.literal_eval(
                text.replace("null", "None")
                .replace("true", "True")
                .replace("false", "False")
            )
        except Exception:
            pass

    # Intento de extracción: buscar el primer objeto JSON {...}
    m = re.search(r"\{.*\}", text, flags=re.S)
    if m:
        candidate = m.group(0)
        try:
            return json.loads(candidate)
        except Exception:
            try:
                return ast.literal_eval(
                    candidate.replace("null", "None")
                    .replace("true", "True")
                    .replace("false", "False")
                )
            except Exception as e:
                raise e


async def validate_python_dict_schema(dict_to_validate: dict, expected_schema: dict):
    try:
        validate(instance=dict_to_validate, schema=expected_schema)
        return True
    except ValidationError as e: # pragma: no cover
        logger.error("Error de validación en schema: %s", str(e))
        return False


async def parse_llm_response_as_python_dict(
    llm_response: str,
    expected_response_dict_schema: dict[str, str],
) -> dict[str, str]:
    logger.info(
        "PARSEANDO RESPUESTA GENERADA POR EL LLM PARA OBTENER UN DICCIONARIO DE PYTHON"
    )
    try:
        execution_status = "SUCCESS"
        the_llm_response_as_dict = json.loads(llm_response)
    except Exception as json_loads_exception: # pragma: no cover
        logger.warning(
            "EL PARSEO DIRECTO DE LOS DATOS USANDO 'JSON LOADS' NO FUNCIONÓ. ERROR: %s",
            str(json_loads_exception),
        )
        logger.warning(
            "A ÇONTINUACIÓN SE MUESTRA LA RESPUESTA DEL MODELO:\n%s",
            llm_response,
        )
        try:
            execution_status = "SUCCESS"
            the_llm_response_as_dict = ast.literal_eval(
                llm_response.replace("null", "None")
                .replace("true", "True")
                .replace("false", "False")
            )
        except Exception as ast_literla_eavl_exception:
            logger.warning(
                "EL PARSEO DIRECTO DE LOS DATOS USANDO 'AST LITERAL EVAL' NO FUNCIONÓ. ERROR: %s",
                str(ast_literla_eavl_exception),
            )
            try:
                execution_status = "SUCCESS"
                the_llm_response_as_dict = await extract_json_string(llm_response)
            except Exception as extract_json_string_exception:
                logger.error(
                    "NO PUDO PARSEARSE LA RESPUESTA DEL MODELO USANDO REGEX. %s",
                    str(extract_json_string_exception),
                )
                execution_status = "ERROR"
                the_llm_response_as_dict = {
                    k: "ERROR"
                    for k in expected_response_dict_schema["properties"].keys()
                }

    # Construimos la respuesta final de la función
    parsed_llm_response_as_dict = {
        "execution_status": execution_status,
        "the_llm_response_as_dict": the_llm_response_as_dict,
    }

    # Solo cuando el parseo funcionó se ejecuta la validación, en caso contrario 'the_llm_response_as_dict' llega cargadita de strings "ERROR"
    if execution_status == "SUCCESS":
        logger.info("EVALUANDO QUE EL SCHEMA GENERADO POR EL MODELO SEA EL CORRECTO")
        validation_result = await validate_python_dict_schema(
            dict_to_validate=the_llm_response_as_dict,
            expected_schema=expected_response_dict_schema,
        )
        if validation_result: # pragma: no cover
            logger.info("RESPUESTA DEL MODELO PARSEADA Y VALIDAD CON ÉXITO.")
            parsed_llm_response_as_dict = {
                "execution_status": execution_status,
                "the_llm_response_as_dict": the_llm_response_as_dict,
            }
        else: # pragma: no cover
            logger.error("EL MODELO NO GENERO UNA REPSUESTA VÁALIDA")
            parsed_llm_response_as_dict = {
                "execution_status": execution_status,
                "the_llm_response_as_dict": {
                    "reasoning_process": "ERROR GENERADO POR LA IA",
                    "dataset_tag": "ERROR",
                },
            }
    return parsed_llm_response_as_dict
