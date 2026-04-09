import argparse
import inspect
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config_banco import DB_CONFIG
from app import gerar_sql

DOWNLOADS_DIR = PROJECT_ROOT / "downloads"
BACKUP_DIR = PROJECT_ROOT / "data" / "db_backups"
AUDIT_DIR = PROJECT_ROOT / "data" / "db_audits"

COMPETENCIAS = {
    "2026-02": "fevereiro-26",
    "2026-03": "marco-26",
}


def detectar_tipo(path: Path):
    partes = path.parts
    if "pagos" in partes:
        return "pagos"
    if "nao-pagos" in partes:
        return "nao_pagos"
    if "novos-locatarios" in partes:
        return "novos_cadastros"
    return None


def chave_logica(row):
    return (
        str(row.get("imobiliaria_id") or ""),
        str(row.get("tipo_planilha") or ""),
        str(row.get("contrato") or ""),
        str(row.get("numero_imovel") or ""),
        str(row.get("competencia") or ""),
        str(row.get("aluguel_vencimento") or ""),
    )


def carregar_planilhas_esperadas():
    seguro_map = gerar_sql.load_seguro_map()
    processar_arquivo = gerar_sql.processar_arquivo
    aceita_seguro_map = len(inspect.signature(processar_arquivo).parameters) >= 5

    esperado_por_bloco = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    esperado_por_chave = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    arquivos_por_bloco = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    erros_parse = []

    for competencia, pasta_mes in COMPETENCIAS.items():
        for path in DOWNLOADS_DIR.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in (".csv", ".xls", ".xlsx"):
                continue
            if pasta_mes not in path.parts:
                continue

            tipo = detectar_tipo(path)
            if not tipo:
                continue

            slug, _, _ = gerar_sql.extrair_metadados(path.name, tipo)
            if not slug:
                continue

            imobiliaria_id, nome_imob = gerar_sql.id_imobiliaria_por_slug(slug)
            if imobiliaria_id is None:
                erros_parse.append({"arquivo": str(path), "erro": "slug_nao_mapeado", "slug": slug})
                continue

            try:
                if aceita_seguro_map:
                    rows = processar_arquivo(str(path), imobiliaria_id, competencia, tipo, seguro_map)
                else:
                    rows = processar_arquivo(str(path), imobiliaria_id, competencia, tipo)
            except Exception as exc:
                erros_parse.append({"arquivo": str(path), "erro": str(exc), "slug": slug})
                continue

            for row in rows:
                esperado_por_bloco[competencia][imobiliaria_id][tipo][row["id"]] = row
                esperado_por_chave[competencia][imobiliaria_id][tipo][chave_logica(row)] = row

            arquivos_por_bloco[competencia][imobiliaria_id][tipo].append({
                "arquivo": str(path),
                "nome_imob": nome_imob,
                "linhas": len(rows),
            })

    return esperado_por_bloco, esperado_por_chave, arquivos_por_bloco, erros_parse


def conectar_dict():
    cfg = dict(DB_CONFIG)
    cfg["cursorclass"] = pymysql.cursors.DictCursor
    cfg["connect_timeout"] = 30
    cfg["read_timeout"] = 600
    cfg["write_timeout"] = 600
    return pymysql.connect(**cfg)


def executar_upsert(cur, row):
    sql = """
    INSERT INTO locatarios
      (id, id_legacy, imobiliaria_id, numero_imovel, contrato, competencia, aluguel_vencimento, tipo_planilha,
       credito_s_multa, vigencia_inicio, vigencia_fim, segurado, risco, coberturas)
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
      coberturas = VALUES(coberturas)
    """
    cur.execute(sql, (
        row["id"],
        row.get("id_legacy") or None,
        row["imobiliaria_id"],
        row.get("numero_imovel") or row.get("id") or None,
        row.get("contrato") or None,
        row["competencia"],
        row.get("aluguel_vencimento") or "",
        row["tipo_planilha"],
        row.get("credito_s_multa") or None,
        row.get("vigencia_inicio") or None,
        row.get("vigencia_fim") or None,
        json.dumps(row.get("segurado") or {}, ensure_ascii=False),
        json.dumps(row.get("risco") or {}, ensure_ascii=False),
        json.dumps(row.get("coberturas") or {}, ensure_ascii=False),
    ))


def salvar_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def ids_como_placeholders(ids):
    return ",".join(["%s"] * len(ids))


def reconciliar(aplicar=False):
    esperado_por_bloco, esperado_por_chave, arquivos_por_bloco, erros_parse = carregar_planilhas_esperadas()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    resumo = {
        "executado_em": datetime.now().isoformat(),
        "competencias": {},
        "parse_errors": erros_parse,
        "pendencias_com_apolice": [],
    }

    conn = conectar_dict()
    try:
        all_loc_rows = []
        all_apolice_rows = []

        with conn.cursor() as cur:
            for competencia in COMPETENCIAS:
                cur.execute(
                    """
                    SELECT *
                    FROM locatarios
                    WHERE competencia = %s
                    """,
                    (competencia,),
                )
                all_loc_rows.extend(cur.fetchall())

                cur.execute(
                    """
                    SELECT a.*, l.competencia, l.imobiliaria_id, l.tipo_planilha
                    FROM apolices_geradas a
                    JOIN locatarios l ON l.id = a.locatario_id
                    WHERE l.competencia = %s
                    """,
                    (competencia,),
                )
                all_apolice_rows.extend(cur.fetchall())

        salvar_json(BACKUP_DIR / f"reconciliacao_fev_mar_{timestamp}_locatarios_before.json", all_loc_rows)
        salvar_json(BACKUP_DIR / f"reconciliacao_fev_mar_{timestamp}_apolices_before.json", all_apolice_rows)

        with conn.cursor() as cur:
            for competencia in COMPETENCIAS:
                resumo["competencias"][competencia] = {}
                imobs = sorted(esperado_por_bloco[competencia].keys())

                for imobiliaria_id in imobs:
                    nome_imob = None
                    tipos = esperado_por_bloco[competencia][imobiliaria_id]
                    resumo["competencias"][competencia][str(imobiliaria_id)] = {
                        "arquivos": arquivos_por_bloco[competencia][imobiliaria_id],
                        "tipos": {},
                    }

                    for tipo, esperado_ids in tipos.items():
                        esperado_rows = esperado_por_bloco[competencia][imobiliaria_id][tipo]
                        esperado_keys = esperado_por_chave[competencia][imobiliaria_id][tipo]

                        cur.execute(
                            """
                            SELECT *
                            FROM locatarios
                            WHERE competencia = %s AND imobiliaria_id = %s AND tipo_planilha = %s
                            """,
                            (competencia, imobiliaria_id, tipo),
                        )
                        db_rows = cur.fetchall()
                        db_ids = {row["id"] for row in db_rows}
                        esperado_set = set(esperado_rows.keys())

                        faltantes = sorted(esperado_set - db_ids)
                        excedentes = sorted(db_ids - esperado_set)

                        remaps = []
                        excedentes_sem_apolice = []
                        pendentes_com_apolice = []

                        if excedentes:
                            fmt = ids_como_placeholders(excedentes)
                            cur.execute(
                                f"""
                                SELECT a.id AS apolice_id, a.locatario_id
                                FROM apolices_geradas a
                                WHERE a.locatario_id IN ({fmt})
                                """,
                                excedentes,
                            )
                            links = cur.fetchall()
                            apolice_por_loc = defaultdict(list)
                            for row in links:
                                apolice_por_loc[row["locatario_id"]].append(row["apolice_id"])

                            for row in db_rows:
                                loc_id = row["id"]
                                if loc_id not in excedentes:
                                    continue
                                apolices = apolice_por_loc.get(loc_id, [])
                                if not apolices:
                                    excedentes_sem_apolice.append(loc_id)
                                    continue
                                key = chave_logica(row)
                                match = esperado_keys.get(key)
                                if match and match["id"] != loc_id:
                                    remaps.append({
                                        "from": loc_id,
                                        "to": match["id"],
                                        "apolices": apolices,
                                    })
                                elif match and match["id"] == loc_id:
                                    pass
                                else:
                                    pendentes_com_apolice.append({
                                        "locatario_id": loc_id,
                                        "apolices": apolices,
                                        "contrato": row.get("contrato"),
                                        "numero_imovel": row.get("numero_imovel"),
                                        "aluguel_vencimento": row.get("aluguel_vencimento"),
                                    })

                        # garante existência dos IDs esperados antes de remapear apólices
                        if aplicar:
                            for loc_id in faltantes:
                                executar_upsert(cur, esperado_rows[loc_id])

                        if aplicar:
                            for remap in remaps:
                                target = esperado_rows.get(remap["to"])
                                if target:
                                    executar_upsert(cur, target)
                                cur.execute(
                                    """
                                    UPDATE apolices_geradas
                                    SET locatario_id = %s
                                    WHERE locatario_id = %s
                                    """,
                                    (remap["to"], remap["from"]),
                                )

                        if aplicar and excedentes_sem_apolice:
                            fmt = ids_como_placeholders(excedentes_sem_apolice)
                            cur.execute(
                                f"DELETE FROM locatarios WHERE id IN ({fmt})",
                                excedentes_sem_apolice,
                            )

                        if aplicar:
                            conn.commit()

                        nome_imob = nome_imob or (
                            arquivos_por_bloco[competencia][imobiliaria_id][tipo][0]["nome_imob"]
                            if arquivos_por_bloco[competencia][imobiliaria_id][tipo]
                            else str(imobiliaria_id)
                        )

                        bloco = {
                            "nome": nome_imob,
                            "expected_count": len(esperado_set),
                            "db_before_count": len(db_ids),
                            "missing_upserted": len(faltantes),
                            "extras_deleted_without_apolice": len(excedentes_sem_apolice),
                            "apolices_remapped": sum(len(item["apolices"]) for item in remaps),
                            "locatarios_remapped": len(remaps),
                            "pending_with_apolice": len(pendentes_com_apolice),
                        }
                        resumo["competencias"][competencia][str(imobiliaria_id)]["tipos"][tipo] = bloco

                        for pendencia in pendentes_com_apolice:
                            resumo["pendencias_com_apolice"].append({
                                "competencia": competencia,
                                "imobiliaria_id": imobiliaria_id,
                                "nome": nome_imob,
                                "tipo": tipo,
                                **pendencia,
                            })

        resumo["modo"] = "apply" if aplicar else "dry_run"
        salvar_json(AUDIT_DIR / f"reconciliacao_fev_mar_{timestamp}_resultado.json", resumo)
        return resumo
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Aplica as alterações no banco.")
    args = parser.parse_args()

    resultado = reconciliar(aplicar=args.apply)
    for competencia, dados in resultado["competencias"].items():
        total_missing = 0
        total_deleted = 0
        total_remap = 0
        total_pending = 0
        for item in dados.values():
            for info in item["tipos"].values():
                total_missing += info["missing_upserted"]
                total_deleted += info["extras_deleted_without_apolice"]
                total_remap += info["apolices_remapped"]
                total_pending += info["pending_with_apolice"]
        print(
            competencia,
            {
                "missing_upserted": total_missing,
                "extras_deleted_without_apolice": total_deleted,
                "apolices_remapped": total_remap,
                "pending_with_apolice": total_pending,
            },
        )
