import sys
import os
from google.cloud import storage
from google.api_core.exceptions import Forbidden, NotFound, GoogleAPIError
from google.auth.exceptions import GoogleAuthError


# Extensões seguras permitidas para ingestão (dados, tabelas, textos, planilhas e documentos)
ALLOWED_EXTENSIONS = {
    # Texto e dados estruturados
    ".csv", ".tsv", ".txt", ".json", ".jsonl", ".xml", ".yaml", ".yml",
    # Arquivos analíticos e colunares
    ".parquet", ".orc", ".avro",
    # Planilhas e documentos de escritório
    ".xlsx", ".xls", ".ods", ".pdf",
    # Compactados comuns de dados
    ".gz", ".zip", ".tar", ".tgz"
}


def is_allowed_file(file_path: str) -> bool:
    """Verifica se a extensão do arquivo está na lista de formatos permitidos."""
    _, ext = os.path.splitext(file_path.lower())
    return ext in ALLOWED_EXTENSIONS


def get_base_path() -> str:
    """Retorna o diretório base onde o .exe ou o script Python está rodando."""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def find_default_json_key() -> str:
    """Busca automaticamente por uma chave JSON no diretório do executável."""
    base_dir = get_base_path()
    default_key = os.path.join(base_dir, "service_account.json")
    if os.path.exists(default_key):
        return default_key

    for file in os.listdir(base_dir):
        if file.endswith(".json") and not file.startswith("."):
            return os.path.join(base_dir, file)

    return ""


def get_asset_path(filename: str) -> str:
    """Retorna o caminho de um arquivo dentro de assets/, compatível com PyInstaller e local."""
    base_dir = get_base_path()
    candidates = [
        os.path.join(base_dir, "assets", filename),
        os.path.join(base_dir, filename),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "assets", filename)),
    ]
    meipass = getattr(sys, '_MEIPASS', None)
    if meipass:
        candidates.insert(0, os.path.join(meipass, "assets", filename))
        candidates.insert(1, os.path.join(meipass, filename))

    for path in candidates:
        if os.path.exists(path):
            return path
    return ""


def get_icon_path() -> str:
    """Retorna o caminho do ícone .ico, compatível com PyInstaller e execução local."""
    return get_asset_path("icon.ico")


def upload_file_to_gcp(json_path: str, file_path: str, bucket_name: str, folder_path: str = "") -> str:
    """Envia um arquivo local para um bucket no GCS com tratamento amigável de exceções."""
    results = upload_files_to_gcp(json_path, [file_path], bucket_name, folder_path)
    return results[0]


def upload_files_to_gcp(
    json_path: str,
    file_paths: list[str],
    bucket_name: str,
    folder_path: str = "",
    progress_callback=None,
    is_simulation: bool = False
) -> list[str]:
    """Envia múltiplos arquivos locais para um bucket no GCS (ou simula envio)."""
    if not file_paths:
        raise ValueError("Nenhum arquivo foi selecionado para upload.")

    invalid_files = [os.path.basename(f) for f in file_paths if not is_allowed_file(f)]
    if invalid_files:
        raise ValueError(
            f"Tipo de arquivo não permitido por segurança:\n{', '.join(invalid_files)}\n\n"
            "Apenas arquivos de dados, textos e documentos são aceitos (ex: .csv, .txt, .json, .parquet, .xlsx, .pdf, .zip, etc.)."
        )

    for f in file_paths:
        if not os.path.exists(f):
            raise FileNotFoundError(f"O arquivo selecionado não foi encontrado no computador:\n{f}")

    clean_folder = folder_path.strip().strip("/")
    total_files = len(file_paths)
    uploaded_uris = []

    # MODO SIMULAÇÃO (Não requer internet, Service Account ou Bucket real)
    if is_simulation:
        import time
        for idx, file_path in enumerate(file_paths, start=1):
            time.sleep(0.7)  # Simula tempo de envio de rede
            file_name = os.path.basename(file_path)
            blob_name = f"{clean_folder}/{file_name}" if clean_folder else file_name
            simulated_uri = f"gs://{bucket_name.strip() or 'bucket-simulado'}/{blob_name} [SIMULADO]"
            uploaded_uris.append(simulated_uri)

            if progress_callback:
                progress_callback(idx, total_files, file_name, simulated_uri)

        return uploaded_uris

    # MODO REAL
    if not os.path.exists(json_path):
        raise FileNotFoundError("O arquivo de credencial JSON selecionado não foi encontrado no computador.")

    # Apontar credencial para o SDK da Google
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name.strip())

        for idx, file_path in enumerate(file_paths, start=1):
            file_name = os.path.basename(file_path)
            blob_name = f"{clean_folder}/{file_name}" if clean_folder else file_name

            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_path)
            uri = f"gs://{bucket_name.strip()}/{blob_name}"
            uploaded_uris.append(uri)

            if progress_callback:
                progress_callback(idx, total_files, file_name, uri)

        return uploaded_uris

    except Forbidden:
        raise PermissionError(
            f"Permissão Negada (403):\nA Service Account informada não tem permissão para gravar no bucket '{bucket_name.strip()}'.\n\n"
            "Verifique se ela possui a role 'Storage Object Creator' configurada no Google Cloud."
        )
    except NotFound:
        raise ValueError(
            f"Bucket Não Encontrado (404):\nO bucket '{bucket_name.strip()}' não existe ou você não possui acesso a ele.\n\n"
            "Confira se o nome do bucket foi digitado corretamente."
        )
    except GoogleAuthError:
        raise ValueError(
            "Credencial Inválida:\nO arquivo .json selecionado não é uma chave válida de Service Account do Google Cloud."
        )
    except GoogleAPIError as e:
        raise RuntimeError(f"Erro na API do Google Cloud:\n{getattr(e, 'message', str(e))}")
    except Exception as e:
        if "Failed to establish a new connection" in str(e) or "Max retries exceeded" in str(e):
            raise ConnectionError(
                "Falha de Conexão:\nNão foi possível se conectar aos servidores do Google Cloud.\n\n"
                "Verifique sua conexão com a internet ou se há bloqueio de proxy/firewall."
            )
        raise RuntimeError(f"Ocorreu um erro inesperado durante o upload:\n{str(e)}")