# =========================
# Inserir/atualizar locatários no MySQL a partir de locatarios_gerados.json
# =========================
# Não deleta registros; usa INSERT ... ON DUPLICATE KEY UPDATE (upsert).
# Commits em lotes para evitar lock timeout e conexão perdida.

import os
import json
import time
import logging
import pymysql

from .config_banco import DB_CONFIG

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_JSON = os.path.join(PROJECT_ROOT, "data", "locatarios_gerados.json")

# Timeouts em segundos (conexão remota pode cair em operações longas)
CONNECT_TIMEOUT = 120
READ_TIMEOUT = 600
WRITE_TIMEOUT = 600
MAX_RETRIES = 5
RETRY_DELAY = 10
# Lock wait: esperar mais antes de tentar de novo
RETRY_DELAY_LOCK = 20
# Commit a cada N linhas dentro do lote (evita lock wait e conexão longa)
COMMIT_A_CADA_LINHAS = 50

SQL_UPSERT = """
INSERT INTO locatarios
  (id, id_legacy, imobiliaria_id, numero_imovel, contrato, competencia, aluguel_vencimento, tipo_planilha, credito_s_multa,
   vigencia_inicio, vigencia_fim, segurado, risco, coberturas)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  id_legacy = VALUES(id_legacy),
  numero_imovel = VALUES(numero_imovel),
  contrato = VALUES(contrato),
  competencia = VALUES(competencia),
  aluguel_vencimento = VALUES(aluguel_vencimento),
  tipo_planilha = VALUES(tipo_planilha),
  credito_s_multa = VALUES(credito_s_multa),
  vigencia_inicio = VALUES(vigencia_inicio),
  vigencia_fim = VALUES(vigencia_fim),
  segurado = VALUES(segurado),
  risco = VALUES(risco),
  coberturas = VALUES(coberturas);
"""


def conectar():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        charset=DB_CONFIG["charset"],
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=CONNECT_TIMEOUT,
        read_timeout=READ_TIMEOUT,
        write_timeout=WRITE_TIMEOUT,
    )


def _executar_um_lote(conn, lote):
    """Executa um único lote (uma imobiliária), com commit a cada N linhas."""
    locs = lote.get("locatarios", [])
    if not locs:
        return
    with conn.cursor() as cur:
        for i, loc in enumerate(locs):
            cur.execute(SQL_UPSERT, (
                loc["id"],
                loc.get("id_legacy") or None,
                loc["imobiliaria_id"],
                loc.get("numero_imovel") or loc.get("id") or None,
                loc.get("contrato") or None,
                loc["competencia"],
                loc.get("aluguel_vencimento") or "",
                loc["tipo_planilha"],
                loc.get("credito_s_multa") or None,
                loc.get("vigencia_inicio") or None,
                loc.get("vigencia_fim") or None,
                json.dumps(loc.get("segurado") or {}, ensure_ascii=False),
                json.dumps(loc.get("risco") or {}, ensure_ascii=False),
                json.dumps(loc.get("coberturas") or {}, ensure_ascii=False),
            ))
            if (i + 1) % COMMIT_A_CADA_LINHAS == 0:
                conn.commit()
        conn.commit()


def run():
    if not os.path.isfile(INPUT_JSON):
        return False
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        lotes = json.load(f)
    if not lotes:
        return False

    logger = logging.getLogger("ENVIOAPI") if logging.getLogger().handlers else logging.getLogger()
    total = len(lotes)
    processados = 0

    for idx, lote in enumerate(lotes):
        if not lote.get("locatarios"):
            continue
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                conn = conectar()
                try:
                    _executar_um_lote(conn, lote)
                    processados += 1
                    if (idx + 1) % 5 == 0 or idx == total - 1:
                        logger.info(f"Lote {idx + 1}/{total} enviado.")
                finally:
                    conn.close()
                if idx < total - 1:
                    time.sleep(2)
                break
            except pymysql.err.OperationalError as e:
                errno = e.args[0] if e.args else 0
                if errno in (2013, 1205, 2006, 2003) and attempt < MAX_RETRIES:
                    delay = RETRY_DELAY_LOCK if errno == 1205 else RETRY_DELAY
                    logger.warning(f"Erro MySQL no lote {idx + 1} (tentativa {attempt}/{MAX_RETRIES}): {e}. Aguardando {delay}s...")
                    time.sleep(delay)
                else:
                    raise

    return processados > 0
