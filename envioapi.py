# =========================
# ENVIO PARA O BANCO (standalone, autossuficiente)
# =========================
# Lê as planilhas em downloads/, gera JSON de locatários e faz upsert no MySQL.
# Após sucesso, exclui os arquivos da pasta. Não depende do main.py nem da API.
#
# Uso: python envioapi.py
#
# Requer que os arquivos sigam o padrão do robô:
#   eventos_totalizador_<SLUG_IMOB>_pagos|nao_pagos_YYYY-MM-DD_HH-MM.csv (ou .xls / .xlsx)
# O SLUG_IMOB é usado para localizar id_imobiliaria em imobiliarias_eventos.

import os
import re
import glob
import logging
import shutil
import unicodedata
from datetime import datetime

ROBO_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(ROBO_DIR, "downloads")
DOWNLOADS_PROCESSADOS_DIR = os.path.join(ROBO_DIR, "downloads_processados")
DOWNLOADS_ENTRADA = {
    "pagos": os.path.join(ROBO_DIR, "downloads_pagos"),
    "nao_pagos": os.path.join(ROBO_DIR, "downloads_nao_pagos"),
    "novas_locacoes": os.path.join(ROBO_DIR, "downloads_novas_locacoes"),
}
COMPETENCIA_FIXA = os.getenv("ROBO_COMPETENCIA_FIXA") or None

from app.imobiliarias_eventos import IMOBILIARIAS_EVENTOS

from app import gerar_sql
from app import insert_banco


def configurar_logger():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    nome_log = datetime.now().strftime(f"{log_dir}/envioapi_%Y-%m-%d_%H-%M-%S.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(nome_log, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("ENVIOAPI")


def slug_nome(s):
    """Gera um nome seguro para arquivo (sem barras, caracteres especiais)."""
    s = str(s).strip()
    s = re.sub(r'[/\\:*?"<>|]', "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:80] if s else "arquivo"


# Padrões aceitos:
# eventos_totalizador_<slug>_pagos|nao_pagos_YYYY-MM-DD_HH-MM.ext
# novas_locacoes_<slug>_YYYY-MM-DD_HH-MM.ext
PATTERN_COM_TIPO = re.compile(
    r"^eventos_totalizador_(.+?)_(nao_pagos|pagos)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(xls|xlsx|csv)$",
    re.IGNORECASE,
)
PATTERN_NOVAS_LOCACOES = re.compile(
    r"^novas_locacoes_(.+)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(xls|xlsx|csv)$",
    re.IGNORECASE,
)
# Padrão antigo (sem tipo no nome): eventos_totalizador_<slug>_YYYY-MM-DD_HH-MM.ext
PATTERN_SEM_TIPO = re.compile(
    r"^eventos_totalizador_(.+)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(xls|xlsx|csv)$",
    re.IGNORECASE,
)


def extrair_slug_competencia_tipo(nome_arquivo):
    """Retorna (slug, competencia YYYY-MM, tipo_planilha) ou (None, None, None)."""
    m = PATTERN_COM_TIPO.match(nome_arquivo)
    if m:
        slug = m.group(1).strip()
        tipo = m.group(2).lower()  # pagos ou nao_pagos
        return slug, (COMPETENCIA_FIXA or m.group(3)[:7]), tipo
    m = PATTERN_NOVAS_LOCACOES.match(nome_arquivo)
    if m:
        slug = m.group(1).strip()
        return slug, (COMPETENCIA_FIXA or m.group(2)[:7]), "novos_cadastros"
    m = PATTERN_SEM_TIPO.match(nome_arquivo)
    if m:
        slug = m.group(1).strip()
        return slug, (COMPETENCIA_FIXA or m.group(2)[:7]), "pagos"
    return None, None, None


def normalizar_slug(s):
    """Lowercase e sem acentos para comparação."""
    s = (s or "").lower().strip()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def id_imobiliaria_por_slug(slug):
    """Retorna id_imobiliaria cujo nome, normalizado, bate com o slug do arquivo.
    O slug do arquivo pode ser 'ADDAD_pagos' ou 'ADDAD' (sem sufixo); removemos _pagos/_nao_pagos
    e comparamos de forma tolerante (ignorando acentos, caixa e underscores extras)."""
    # Remove sufixo de tipo se por acaso vier grudado ao slug
    slug_limpo = re.sub(r"_(pagos|nao_pagos)$", "", slug, flags=re.IGNORECASE).strip()
    slug_norm = normalizar_slug(slug_limpo)

    for imob in IMOBILIARIAS_EVENTOS:
        nome = imob.get("nome", "")
        nome_norm = normalizar_slug(slug_nome(nome))

        # Match exato normalizado
        if nome_norm == slug_norm:
            return imob.get("id_imobiliaria"), imob.get("nome")

        # Match mais flexível: desconsidera underscores / espaços
        if nome_norm.replace("_", "") == slug_norm.replace("_", ""):
            return imob.get("id_imobiliaria"), imob.get("nome")
        if slug_norm.startswith(nome_norm + "_") or slug_norm.replace("_", "").startswith(nome_norm.replace("_", "")):
            return imob.get("id_imobiliaria"), imob.get("nome")

    return None, None


def listar_planilhas_entrada():
    arquivos = []
    for tipo_planilha, pasta in DOWNLOADS_ENTRADA.items():
        if not os.path.isdir(pasta):
            continue
        for ext in ("*.xls", "*.xlsx", "*.csv"):
            for caminho in glob.glob(os.path.join(pasta, "**", ext), recursive=True):
                arquivos.append((caminho, tipo_planilha))
    for ext in ("*.xls", "*.xlsx", "*.csv"):
        for caminho in glob.glob(os.path.join(DOWNLOAD_DIR, "**", ext), recursive=True):
            caminho_abs = os.path.abspath(caminho)
            if caminho_abs.startswith(os.path.abspath(DOWNLOADS_PROCESSADOS_DIR) + os.sep):
                continue
            arquivos.append((caminho, None))
    vistos = set()
    unicos = []
    for caminho, tipo_planilha in arquivos:
        chave = os.path.abspath(caminho)
        if chave in vistos:
            continue
        vistos.add(chave)
        unicos.append((caminho, tipo_planilha))
    return unicos


def caminho_unico(destino_dir, nome_arquivo):
    base, ext = os.path.splitext(nome_arquivo)
    caminho = os.path.join(destino_dir, nome_arquivo)
    contador = 1
    while os.path.exists(caminho):
        caminho = os.path.join(destino_dir, f"{base}_{contador}{ext}")
        contador += 1
    return caminho


def destino_arquivo_processado(caminho, tipo_planilha):
    caminho_abs = os.path.abspath(caminho)
    downloads_abs = os.path.abspath(DOWNLOAD_DIR)
    if caminho_abs.startswith(downloads_abs + os.sep):
        relativo = os.path.relpath(caminho_abs, downloads_abs)
        return os.path.join(DOWNLOADS_PROCESSADOS_DIR, relativo)

    nome_arquivo = os.path.basename(caminho)
    tipo_dir = (tipo_planilha or "outros").replace("_", "-")
    return os.path.join(DOWNLOADS_PROCESSADOS_DIR, tipo_dir, nome_arquivo)


def arquivar_planilha(caminho, tipo_planilha, logger):
    if not os.path.isfile(caminho):
        return
    destino_base = destino_arquivo_processado(caminho, tipo_planilha)
    destino_dir = os.path.dirname(destino_base)
    os.makedirs(destino_dir, exist_ok=True)
    destino = caminho_unico(destino_dir, os.path.basename(destino_base))
    try:
        shutil.move(caminho, destino)
        logger.info(f"Planilha arquivada: {os.path.relpath(destino, ROBO_DIR)}")
    except OSError as e:
        logger.warning(f"Não foi possível arquivar {caminho}: {e}")


def main():
    logger = configurar_logger()
    logger.info("Envio para o banco (planilhas separadas por tipo)")
    if COMPETENCIA_FIXA:
        logger.info(f"Competência fixa ativa: {COMPETENCIA_FIXA}")

    para_envio = []
    for caminho, tipo_origem in listar_planilhas_entrada():
        nome = os.path.basename(caminho)
        slug, competencia, tipo_planilha = extrair_slug_competencia_tipo(nome)
        if not slug or not competencia:
            continue
        if tipo_planilha is None:
            tipo_planilha = tipo_origem
        if tipo_planilha == "novas_locacoes":
            tipo_planilha = "novos_cadastros"
        if tipo_planilha not in ("pagos", "nao_pagos", "novos_cadastros"):
            continue
        id_imob, nome_imob = id_imobiliaria_por_slug(slug)
        if id_imob is None:
            logger.warning(f"Imobiliária não encontrada para o arquivo: {nome}")
            continue
        para_envio.append({
            "caminho": caminho,
            "id_imobiliaria": id_imob,
            "competencia": competencia,
            "tipo_planilha": tipo_planilha,
            "nome_imob": nome_imob,
        })

    if not para_envio:
        logger.info("Nenhuma planilha elegível (pagos/nao_pagos/novos_cadastros) na pasta para envio.")
        return

    logger.info(f"Processando {len(para_envio)} planilha(s) — gerando JSON e enviando ao banco...")
    saida = gerar_sql.main()
    if insert_banco.run():
        for item in para_envio:
            arquivar_planilha(item["caminho"], item["tipo_planilha"], logger)
        # Quantidade de registros por imobiliária
        nome_por_id = {item["id_imobiliaria"]: item["nome_imob"] for item in para_envio}
        contagem = {}
        for lote in (saida or []):
            id_imob = lote.get("imobiliaria_id")
            n = len(lote.get("locatarios", []))
            contagem[id_imob] = contagem.get(id_imob, 0) + n
        logger.info("Quantidade por imobiliária:")
        for id_imob in sorted(contagem.keys()):
            nome = nome_por_id.get(id_imob, f"id_{id_imob}")
            logger.info(f"  {nome} (id {id_imob}): {contagem[id_imob]} registro(s)")
        logger.info(f"Total: {sum(contagem.values())} registro(s)")
        logger.info("Envio finalizado.")
    else:
        logger.warning("Nenhum dado para inserir ou arquivo locatarios_gerados.json não encontrado.")


if __name__ == "__main__":
    main()
