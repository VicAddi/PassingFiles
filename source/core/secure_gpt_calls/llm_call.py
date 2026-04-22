import asyncio
import logging
import time
import traceback
from typing import Any

import openai

from ...config import AXA_SECURE_GPT_LLM_ID
from ...services.securegpt import secure_gpt as SecureGPTFunctions

logger = logging.getLogger()


async def execute_call_to_secure_gpt_llm(
    system_prompt: str,
    model_id: str = AXA_SECURE_GPT_LLM_ID,
    max_retries: int = 10,
) -> dict[str, Any]:
    """
    Llama al modelo Secure GPT (Azure OpenAI) con reintentos ante rate-limiting (429).

    Args:
        system_prompt: Contenido para el rol 'system' del prompt.
        model_id: ID del modelo a usar (por defecto AXA_SECURE_GPT_LLM_ID).
        max_retries: Número máximo de reintentos ante RateLimitExceeded.
    Returns:
        Un diccionario con la representación de la respuesta del completion.
    """
    if not system_prompt: 
        raise ValueError("system_prompt must be provided") #pragma: no cover

    # Instanciar el cliente (según tu bloque original)
    secure_gpt_llm_client = (
        SecureGPTFunctions.get_secure_gpt_async_az_openai_llm_client_sdk()
    )

    messages = [{"role": "system", "content": system_prompt}]

    attempt = 0
    while True: # pragma: no cover
        try: 
            secure_gpt_llm_completion = (
                await secure_gpt_llm_client.chat.completions.create(
                    model=model_id,
                    messages=messages,
                )
            )
            completion_dict = secure_gpt_llm_completion.to_dict()
            logger.info(
                "RESPUESTA DEL MODELO RECIBIDA CON ÉXITO. INTENTOS EJECUTADOS: %s",
                f"{attempt+1:,}",
            )
            return completion_dict
        except Exception as exc:
            logger.warning("HA OCURRIDO UNA EXCEPCIÓN AL EJECUTAR LLAMADA AL LLM.")
            # Detectar rate limit (429) o código de error 'RateLimitExceeded'
            is_rate_limit = False

            # 1) Código de estado HTTP 429 (si está disponible)
            status_code = getattr(exc, "http_status", None) or getattr(
                exc, "status", None
            )
            if status_code == 429:
                logger.warning(
                    "SE HA IDENTIFICADO RATE LIMIT (HTTP 429). PROCEDIENDO A REINTENTAR"
                )
                is_rate_limit = True

            # 2) Código de error NO_QUOTA o texto NO_QUOTA en la excepción
            if not is_rate_limit:
                if getattr(exc, "code", None) == "NO_QUOTA":
                    logger.warning(
                        "SE HA IDENTIFICADO NO_QUOTA EN LA EXCEPCIÓN. PROCEDIENDO A REINTENTAR"
                    )
                    is_rate_limit = True
                else:
                    exc_str = str(exc)
                    if "NO_QUOTA" in exc_str or "no quota" in exc_str.lower():
                        logger.warning(
                            "SE HA IDENTIFICADO NO_QUOTA EN EL MENSAJE DE LA EXCEPCIÓN. PROCEDIENDO A REINTENTAR"
                        )
                        is_rate_limit = True

            # 3) Si OpenAI RateLimitError es parte del stack (si está disponible)
            if not is_rate_limit and openai is not None:
                try:
                    RateLimitError = getattr(openai, "RateLimitError", None)
                    if RateLimitError is not None and isinstance(exc, RateLimitError):
                        logger.warning(
                            "SE HA IDENTIFICADO openai.RateLimitError. REINTENTANDO"
                        )
                        is_rate_limit = True
                except Exception:
                    pass

            if not is_rate_limit:
                # No es un error de rate limit; propaga la excepción
                error_details = traceback.format_exc()
                logger.error(
                    "ERROR EN LA LLAMADA AL LLM: %s\nDETALLES DEL ERROR:\n%s",
                    exc,
                    error_details,
                )
                raise

            # Rate limit detected: intentar leer Retry-After si está disponible
            retry_after = None
            resp = getattr(exc, "response", None)
            if resp is not None:
                headers = getattr(resp, "headers", None)
                if headers:
                    retry_after = headers.get("Retry-After") or headers.get(
                        "retry-after"
                    )

            if retry_after is not None:
                logger.info(
                    "Se encontró Retry-After en la respuesta. Esperando %s segundos para el siguiente reintento.",
                    retry_after,
                )
                try:
                    delay = float(retry_after)
                except (ValueError, TypeError):
                    logger.warning(
                        "No se pudo parsear Retry-After.Esperando %s minutos.",
                        f"{attempt + 1:,}",
                    )
                    delay = 60 * (attempt + 1)
            else:
                # Delay de 'n' minutos en función delos intentos
                delay = 60 * (attempt + 1)
                logger.warning(
                    "No se encontró Retry-After en la respuesta. Esperando %s minutos.",
                    f"{attempt + 1:,}",
                )

            if attempt >= max_retries:
                # Se excede el número de reintentos; lanzar excepción
                error_details = traceback.format_exc()
                logger.error(
                    "¡MÁXIMO DE INTENTOS ALCANZADO (%s)! \nERROR:%s\nDETALLES DEL ERROR:\n%s",
                    f"{max_retries:,.2f}",
                    exc,
                    error_details,
                )
                raise

            await asyncio.sleep(delay)
            attempt += 1
