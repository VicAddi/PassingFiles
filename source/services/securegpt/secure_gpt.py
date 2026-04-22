import os
import time

import httpx
import requests
from langchain_core.embeddings import Embeddings
from openai import AsyncAzureOpenAI, AzureOpenAI
from pydantic import BaseModel

from ...config import (
    APP_ENVIRONMENT,
    AXA_SECURE_GPT_BASE_ENDPOINT,
    AXA_SECURE_GPT_CLIENT_ID,
    AXA_SECURE_GPT_CLIENT_SECRET,
    AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID,
    AXA_SECURE_GPT_LLM_ID,
    AXA_SECURE_GPT_ONE_ACCOUNT_URL,
    AXA_SECURE_GPT_OPENAI_API_VERSION,
    AXA_SECURE_GPT_OPENAI_BASE_EMBEDDINGS_ENDPOINT,
    AXA_SECURE_GPT_OPENAI_BASE_LLM_ENDPOINT,
    AZURE_OPENAI_EMBEDDINGS_DIMENSION,
    LOGGING_LEVEL,
)
from ...utils.logger import get_logger

logger = get_logger(level=LOGGING_LEVEL)

client = httpx.Client(verify=False if APP_ENVIRONMENT else True)
aclient = httpx.AsyncClient(verify=False if APP_ENVIRONMENT else True)


def get_auth_token_axa_secure_gpt_one_account():
    """
    Obtiene el token de autenticación para SecureGPT y lo guarda como variable de entorno.
    Returns:
        str: El token de autenticación para SecureGPT.
    """
    # Directo a OneAccount (prestar atención a la terminación)
    one_account_url = f"https://{AXA_SECURE_GPT_ONE_ACCOUNT_URL}/as/token.oauth2"
    payload = {
        "client_id": AXA_SECURE_GPT_CLIENT_ID,
        "client_secret": AXA_SECURE_GPT_CLIENT_SECRET,
        "scope": "urn:grp:chatgpt",
        "grant_type": "client_credentials",
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    # Una vez construídos los HEADERS podemos ejecutar la petición
    s_t = time.time()
    logger.info("[ONE ACCOUNT] Obteniendo token de autorización One Account")
    response = requests.post(
        url=one_account_url,
        data=payload,
        headers=headers,
        timeout=60,
        verify=True if APP_ENVIRONMENT is None else False,
    )
    e_t = time.time()
    logger.info(
        "[ONE ACCOUNT] Obteniendo token de autorización obtenido en %.4f segundos",
        e_t - s_t,
    )
    if response.status_code == 200:
        # Debemos validar que la información de "identificación" que envíamos
        # coincida con la que recibimos, de ahí los assert
        response_data = response.json()
        # Guardamos los resutlados del request como variables de
        os.environ["AXA_SECURE_GPT_TOKEN_DURATION"] = str(response_data["expires_in"])
        os.environ["AXA_SECURE_GPT_TOKEN_INIT_TIME"] = str(int(time.time()))
        os.environ["AXA_SECURE_GPT_ACCESS_TOKEN"] = str(response_data["access_token"])
    else:
        # Cualquier status distinto a 200 será arrojado como un excepción
        response.raise_for_status() # pragma: no cover
    return response_data["access_token"]


def get_auth_token() -> str:
    """
    Obtiene un token de autenticación para el servicio AXA Secure GPT.
    Si el token actual no es válido, ha expirado o no está configurado,
    se genera un nuevo token utilizando la función `get_auth_token_axa_secure_gpt_one_account`.
    Returns:
        str: El token de autenticación válido.
    Variables de entorno:
        AXA_SECURE_GPT_TOKEN_DURATION (int): Duración del token en segundos.
        AXA_SECURE_GPT_TOKEN_INIT_TIME (int): Tiempo de inicio del token en formato epoch.
        AXA_SECURE_GPT_ACCESS_TOKEN (str): Token de acceso actual.
    """
    token_duration = int(os.getenv("AXA_SECURE_GPT_TOKEN_DURATION", "0"))
    token_init_time = int(os.getenv("AXA_SECURE_GPT_TOKEN_INIT_TIME", "0"))
    auth_token = os.getenv("AXA_SECURE_GPT_ACCESS_TOKEN", None)
    if (
        auth_token is None
        or ((int(time.time()) - token_init_time) >= (token_duration - 60))
        or (token_duration == 0)
        or (token_init_time == 0)
    ):
        auth_token = get_auth_token_axa_secure_gpt_one_account()
    return auth_token


def get_secure_gpt_az_openai_embeddings_client_sdk( 
    secure_gpt_base_endpoint: str = AXA_SECURE_GPT_BASE_ENDPOINT,
    secure_gpt_openai_embeddings_base_endpoint: str = AXA_SECURE_GPT_OPENAI_BASE_EMBEDDINGS_ENDPOINT,
    secure_gpt_openai_embeddings_model_id: str = AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID,
    secure_gpt_openai_api_version: str = AXA_SECURE_GPT_OPENAI_API_VERSION,
) -> AzureOpenAI: # pragma: no cover
    """
    Crea y configura un cliente de Azure OpenAI para obtener embeddings de OpenAI a través de SecureGPT.
    Returns:
        AzureOpenAI: Cliente configurado para interactuar con el servicio de embeddings de Azure OpenAI.
    """
    httpx_client = httpx.Client( 
        http2=True, verify=True if APP_ENVIRONMENT is None else False, timeout=(30, 180)
    )
    # Obtenemos el token de autenticación
    auth_token = get_auth_token() 
    # Construimos el endpoint
    embeddings_endpoint = (
        secure_gpt_base_endpoint
        + secure_gpt_openai_embeddings_base_endpoint.format(
            secure_gpt_openai_embeddings_model_id, secure_gpt_openai_api_version
        )
    )
    client = AzureOpenAI(
        azure_endpoint=embeddings_endpoint,
        api_key=auth_token,
        api_version=secure_gpt_openai_api_version,
        http_client=httpx_client,
    )
    return client


def get_secure_gpt_az_openai_llm_client_sdk(
    secure_gpt_base_endpoint: str = AXA_SECURE_GPT_BASE_ENDPOINT,
    secure_gpt_openai_llm_base_endpoint: str = AXA_SECURE_GPT_OPENAI_BASE_LLM_ENDPOINT,
    secure_gpt_openai_llm_model_id: str = AXA_SECURE_GPT_LLM_ID,
    secure_gpt_openai_api_version: str = AXA_SECURE_GPT_OPENAI_API_VERSION,
) -> AzureOpenAI: # pragma: no cover
    """
    Crea y configura un cliente AzureOpenAI para interactuar con LLMs de OpenAI a través de SecureGPT.
    Returns:
        AzureOpenAI: Cliente configurado para interactuar con el modelo de lenguaje seguro de GPT de AXA.
    """
    httpx_client = httpx.Client( 
        http2=True, verify=True if APP_ENVIRONMENT is None else False, timeout=(30, 180)
    )
    # Obtenemos el token de autenticación
    auth_token = get_auth_token()
    # Construimos el endpoint
    llm_endpoint = (
        secure_gpt_base_endpoint
        + secure_gpt_openai_llm_base_endpoint.format(
            secure_gpt_openai_llm_model_id, secure_gpt_openai_api_version
        )
    )
    # Construimos el cliente
    client = AzureOpenAI(
        azure_endpoint=llm_endpoint,
        api_key=auth_token,
        api_version=secure_gpt_openai_api_version,
        http_client=httpx_client,
    )
    return client


def get_secure_gpt_async_az_openai_llm_client_sdk(
    secure_gpt_base_endpoint: str = AXA_SECURE_GPT_BASE_ENDPOINT,
    secure_gpt_openai_llm_base_endpoint: str = AXA_SECURE_GPT_OPENAI_BASE_LLM_ENDPOINT,
    secure_gpt_openai_llm_model_id: str = AXA_SECURE_GPT_LLM_ID,
    secure_gpt_openai_api_version: str = AXA_SECURE_GPT_OPENAI_API_VERSION,
):
    """
    Obtiene una instancia asíncrona del cliente de Azure OpenAI LLM.
    Returns:
        AsyncAzureOpenAI: Una instancia asíncrona del cliente de Azure OpenAI LLM.
    """
    httpx_client = httpx.AsyncClient(
        http2=True, verify=True if APP_ENVIRONMENT is None else False, timeout=(30, 180)
    )
    # Obtenemos el token de autenticación
    auth_token = get_auth_token()
    # Construimos el endpoint
    llm_endpoint = (
        secure_gpt_base_endpoint
        + secure_gpt_openai_llm_base_endpoint.format(
            secure_gpt_openai_llm_model_id, secure_gpt_openai_api_version
        )
    )
    # Construimos el cliente
    client = AsyncAzureOpenAI(
        azure_endpoint=llm_endpoint,
        api_key=auth_token,
        api_version=secure_gpt_openai_api_version,
        http_client=httpx_client,
    )
    return client


class PrivateAPIEmbeddings(BaseModel, Embeddings): # pragma: no cover
    """
    Custom Langchain embeddings class for SecureGPT embeddings service.
    """

    def _create_payload(self, texts: list[str]) -> dict: 
        """Creates request payload for the API."""
        return {
            "input": texts,
            "model": AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID,
            "dimensions": AZURE_OPENAI_EMBEDDINGS_DIMENSION,
        }

    async def _get_headers(self) -> dict: 
        """Creates the authorization headers for the API"""
        auth_token = get_auth_token()
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth_token}",
        }

    def _embed_with_retry(self, texts: list[str]) -> list[list[float]]: 
        """Internal syncronous method with retry logic"""
        payload = self._create_payload(texts)
        headers = self._get_headers()

        embeddings_endpoint = (
            AXA_SECURE_GPT_BASE_ENDPOINT
            + AXA_SECURE_GPT_OPENAI_BASE_EMBEDDINGS_ENDPOINT.format(
                AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID, AXA_SECURE_GPT_OPENAI_API_VERSION
            )
        )

        response = client.post(
            embeddings_endpoint, headers=headers, json=payload, timeout=(30, 1200)
        )

        if response.status_code != 200:
            raise ValueError(
                f"Embeddings API request failed with status {response.status_code}: {response.text}"
            )
        response_json = response.json()
        return [item["embedding"] for item in response_json["data"]]

    def embed_documents(self, texts: list[str]) -> list[list[float]]: # pragma: no cover
        """Embedd a list of documents"""
        return self._embed_with_retry(texts)

    def embed_query(self, text):
        """Embed a single query"""
        embeddings = self.embed_documents([text])[0]
        return embeddings

    async def _aembed_with_retry(self, texts: list[str]) -> list[list[float]]:
        """Internal asyncronous method with retry logic"""
        payload = self._create_payload(texts)
        headers = self._get_headers()

        embeddings_endpoint = (
            AXA_SECURE_GPT_BASE_ENDPOINT
            + AXA_SECURE_GPT_OPENAI_BASE_EMBEDDINGS_ENDPOINT.format(
                AXA_SECURE_GPT_EMBEDDINGS_MODEL_ID, AXA_SECURE_GPT_OPENAI_API_VERSION
            )
        )

        response = await aclient.post(
            embeddings_endpoint, headers=headers, json=payload, timeout=(30, 1200)
        )

        if response.status_code != 200:
            raise ValueError(
                f"API request failed with status {response.status_code}: {response.text}"
            )
        response_json = response.json()

        return [item["embedding"] for item in response_json["data"]]

    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        """Asynchronously embed a list of documents"""
        return await self._aembed_with_retry(texts)

    async def aembed_query(self, text: str) -> list[float]:
        """Asynchronously embed a single query"""
        embeddings = await self.aembed_documents([text])
        return embeddings[0]
