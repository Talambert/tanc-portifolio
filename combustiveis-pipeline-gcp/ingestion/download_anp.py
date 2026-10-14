"""Baixa os arquivos da ANP listados em common/sources.yaml para uma pasta local.

Uso (a partir de combustiveis-pipeline-gcp/):
    python ingestion/download_anp.py --check                 # só verifica se as URLs respondem
    python ingestion/download_anp.py --dest data/raw         # baixa o que ainda não existe
    python ingestion/download_anp.py --only ca-2026-01 --unzip

O servidor da ANP bloqueia clientes sem User-Agent de navegador e responde 403 a HEAD,
por isso tudo é feito com GET.
"""
import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SOURCES_FILE = ROOT / "common" / "sources.yaml"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
MANIFEST_NAME = "manifest.json"
CHUNK = 1024 * 1024


def load_sources(only: list[str] | None) -> tuple[str, list[dict]]:
    config = yaml.safe_load(SOURCES_FILE.read_text(encoding="utf-8"))
    sources = config["sources"]
    if only:
        wanted = set(only)
        sources = [s for s in sources if s["id"] in wanted]
        missing = wanted - {s["id"] for s in sources}
        if missing:
            sys.exit(f"Fontes não encontradas em sources.yaml: {', '.join(sorted(missing))}")
    return config["base_url"].rstrip("/"), sources


def open_url(url: str, retries: int = 3):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, retries + 1):
        try:
            return urllib.request.urlopen(request, timeout=60)
        except urllib.error.HTTPError as exc:
            # 4xx não adianta repetir
            if 400 <= exc.code < 500 or attempt == retries:
                raise
        except urllib.error.URLError:
            if attempt == retries:
                raise
        time.sleep(2**attempt)


def check(base_url: str, sources: list[dict]) -> bool:
    ok = True
    for source in sources:
        url = f"{base_url}/{source['path']}"
        try:
            with open_url(url) as response:
                size = int(response.headers.get("Content-Length", 0))
                print(f"OK    {source['id']:<26} {response.status} {size / 1_048_576:7.1f} MB")
        except Exception as exc:  # noqa: BLE001 - relatório de diagnóstico
            ok = False
            print(f"FALHA {source['id']:<26} {exc}")
    return ok


def download(base_url: str, source: dict, dest: Path, force: bool) -> dict:
    target = dest / Path(source["path"]).name
    if target.exists() and not force:
        print(f"já existe  {target.name}")
        status = "ja_existia"
    else:
        url = f"{base_url}/{source['path']}"
        partial = target.with_suffix(target.suffix + ".part")
        with open_url(url) as response, partial.open("wb") as out:
            while chunk := response.read(CHUNK):
                out.write(chunk)
        partial.replace(target)
        print(f"baixado    {target.name}")
        status = "baixado"

    digest = hashlib.sha256()
    with target.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
    return {
        "id": source["id"],
        "arquivo": target.name,
        "bytes": target.stat().st_size,
        "sha256": digest.hexdigest(),
        "status": status,
        "registrado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def unzip(archive: Path) -> list[str]:
    extracted = []
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            # Os nomes dentro do ZIP vêm em cp437/latin-1; corrige para o nome real
            try:
                name = info.filename.encode("cp437").decode("utf-8")
            except UnicodeError:
                name = info.filename
            safe_name = Path(name).name  # evita caminhos fora da pasta
            (archive.parent / safe_name).write_bytes(zf.read(info))
            extracted.append(safe_name)
    return extracted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--dest", default="data/raw", help="pasta de destino (padrão: data/raw)")
    parser.add_argument("--only", nargs="*", help="ids de sources.yaml a baixar (padrão: todos)")
    parser.add_argument("--check", action="store_true", help="só verifica se as URLs respondem")
    parser.add_argument("--unzip", action="store_true", help="descompacta os ZIPs após o download")
    parser.add_argument("--force", action="store_true", help="baixa de novo mesmo se o arquivo existir")
    args = parser.parse_args()

    base_url, sources = load_sources(args.only)

    if args.check:
        return 0 if check(base_url, sources) else 1

    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    # O manifesto acumula entradas por id; rodar com --only não apaga as demais
    manifest_path = dest / MANIFEST_NAME
    entries = {}
    if manifest_path.exists():
        entries = {e["id"]: e for e in json.loads(manifest_path.read_text(encoding="utf-8"))}

    for source in sources:
        entry = download(base_url, source, dest, args.force)
        previous = entries.get(source["id"], {})
        if args.unzip and entry["arquivo"].endswith(".zip"):
            entry["extraidos"] = unzip(dest / entry["arquivo"])
        elif "extraidos" in previous:
            entry["extraidos"] = previous["extraidos"]
        entries[source["id"]] = entry

    manifest_path.write_text(json.dumps(list(entries.values()), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nManifesto gravado em {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
