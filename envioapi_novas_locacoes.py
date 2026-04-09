#!/usr/bin/env python3
# =========================
# ENVIO DIRETO - NOVAS LOCAÇÕES
# =========================
# Lê diretamente os arquivos em downloads_novas_locacoes, gera JSON
# via gerar_sql.py e envia ao banco via insert_banco.py.
#
# Uso:
#   python envioapi_novas_locacoes.py

import glob
import inspect
import json
import logging
import os
import re
import shutil
import sys
import unicodedata
import csv
from datetime import datetime

from app import gerar_sql
from app import insert_banco
from app.imobiliarias_eventos import IMOBILIARIAS_EVENTOS


def configurar_logger():
    robo_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(robo_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    nome_log = datetime.now().strftime(os.path.join(log_dir, "envioapi_novas_locacoes_%Y-%m-%d_%H-%M-%S.log"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(nome_log, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger("ENVIOAPI_NOVAS")


def listar_planilhas_novas_locacoes(pasta):
    arquivos = []
    for ext in ("*.csv", "*.xls", "*.xlsx"):
        arquivos.extend(glob.glob(os.path.join(pasta, "**", ext), recursive=True))
    downloads_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
    downloads_processados_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads_processados")
    for ext in ("*.csv", "*.xls", "*.xlsx"):
        for caminho in glob.glob(os.path.join(downloads_root, "**", ext), recursive=True):
            caminho_abs = os.path.abspath(caminho)
            if caminho_abs.startswith(os.path.abspath(downloads_processados_root) + os.sep):
                continue
            arquivos.append(caminho)
    arquivos = sorted({os.path.abspath(caminho) for caminho in arquivos})
    return sorted(arquivos)


def normalizar_slug(s):
    s = (s or "").lower().strip()
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")
    return s


def slug_nome(s):
    s = str(s).strip()
    s = re.sub(r'[/\\:*?"<>|]', "_", s)
    s = re.sub(r"\s+", "_", s)
    return s


def id_imobiliaria_por_slug(slug):
    slug_norm = normalizar_slug(slug)
    for imob in IMOBILIARIAS_EVENTOS:
        nome_norm = normalizar_slug(slug_nome(imob.get("nome", "")))
        if nome_norm == slug_norm or nome_norm.replace("_", "") == slug_norm.replace("_", ""):
            return imob.get("id_imobiliaria"), imob.get("nome")
    return None, None


def extrair_slug_competencia(nome_arquivo):
    pattern = re.compile(
        r"^novas_locacoes_(.+)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(csv|xls|xlsx)$",
        re.IGNORECASE,
    )
    m = pattern.match(nome_arquivo)
    if not m:
        return None, None
    slug = m.group(1).strip()
    competencia = m.group(2)[:7]
    return slug, competencia


def carregar_seguro_map():
    if hasattr(gerar_sql, "load_seguro_map"):
        return gerar_sql.load_seguro_map()
    return {}


def aumentar_limite_csv():
    # Algumas planilhas têm campos muito longos; evita
    # "field larger than field limit (131072)".
    limite = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limite)
            return
        except OverflowError:
            limite = limite // 10


def gerar_lotes_novas_locacoes(planilhas, logger):
    lotes = []
    seguro_map = carregar_seguro_map()
    competencia_forcada = getattr(gerar_sql, "COMPETENCIA_FORCADA", None)
    processar_arquivo = getattr(gerar_sql, "processar_arquivo")
    aceita_seguro_map = len(inspect.signature(processar_arquivo).parameters) >= 5

    for caminho in planilhas:
        nome = os.path.basename(caminho)
        slug, competencia = extrair_slug_competencia(nome)
        if not slug or not competencia:
            logger.warning(f"Arquivo fora do padrão de novas locações: {nome}")
            continue
        if competencia_forcada:
            competencia = competencia_forcada

        id_imob, nome_imob = id_imobiliaria_por_slug(slug)
        if id_imob is None:
            logger.warning(f"Imobiliária não encontrada para: {nome} (slug: {slug})")
            continue

        try:
            if aceita_seguro_map:
                locs = processar_arquivo(caminho, id_imob, competencia, "novos_cadastros", seguro_map)
            else:
                locs = processar_arquivo(caminho, id_imob, competencia, "novos_cadastros")
        except Exception as e:
            logger.warning(f"Falha ao processar {nome}: {e}")
            continue

        lotes.append(
            {
                "arquivo": nome,
                "imobiliaria_id": id_imob,
                "imobiliaria_nome": nome_imob,
                "competencia": competencia,
                "tipo_planilha": "novos_cadastros",
                "locatarios": locs,
            }
        )
    return lotes


def caminho_unico(destino_dir, nome_arquivo):
    base, ext = os.path.splitext(nome_arquivo)
    caminho = os.path.join(destino_dir, nome_arquivo)
    contador = 1
    while os.path.exists(caminho):
        caminho = os.path.join(destino_dir, f"{base}_{contador}{ext}")
        contador += 1
    return caminho


def arquivar_planilhas(planilhas, pasta_arquivo, logger):
    os.makedirs(pasta_arquivo, exist_ok=True)
    for caminho in planilhas:
        if not os.path.isfile(caminho):
            continue
        destino = caminho_unico(pasta_arquivo, os.path.basename(caminho))
        try:
            shutil.move(caminho, destino)
            logger.info(f"Planilha arquivada: {os.path.basename(destino)}")
        except OSError as e:
            logger.warning(f"Não foi possível arquivar {caminho}: {e}")


def main():
    logger = configurar_logger()
    aumentar_limite_csv()
    robo_dir = os.path.dirname(os.path.abspath(__file__))
    pasta_entrada = os.path.join(robo_dir, "downloads_novas_locacoes")
    pasta_arquivo = os.path.join(robo_dir, "downloads_processados", "novas_locacoes")

    if not os.path.isdir(pasta_entrada):
        logger.error(f"Pasta não encontrada: {pasta_entrada}")
        sys.exit(1)

    planilhas = listar_planilhas_novas_locacoes(pasta_entrada)
    if not planilhas:
        logger.info(f"Nenhuma planilha encontrada em: {pasta_entrada}")
        sys.exit(0)

    logger.info(f"Planilhas encontradas em downloads_novas_locacoes: {len(planilhas)}")
    for caminho in planilhas:
        logger.info(f" - {os.path.basename(caminho)}")

    saida = gerar_lotes_novas_locacoes(planilhas, logger)
    if not saida:
        logger.warning("Nenhuma planilha elegível de novas locações para geração do JSON.")
        sys.exit(0)

    output_json = getattr(gerar_sql, "OUTPUT_JSON", os.path.join(robo_dir, "locatarios_gerados.json"))
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(saida, f, ensure_ascii=False, indent=2)

    total_registros = sum(len(lote.get("locatarios", [])) for lote in saida)
    logger.info(f"Lotes gerados: {len(saida)} | Registros: {total_registros}")

    if insert_banco.run():
        arquivar_planilhas(planilhas, pasta_arquivo, logger)
        logger.info("Envio de novas locações finalizado com sucesso.")
    else:
        logger.warning("Falha no envio ao banco: nenhum dado inserido ou JSON não encontrado.")


if __name__ == "__main__":
    main()
