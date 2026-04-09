import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config_banco import DB_CONFIG
from app.gerar_sql import _sanitize_id_part


BACKUP_DIR = PROJECT_ROOT / "data" / "db_backups"
AUDIT_DIR = PROJECT_ROOT / "data" / "db_audits"
COMMIT_BATCH = 500


def conectar_dict():
    cfg = dict(DB_CONFIG)
    cfg["cursorclass"] = pymysql.cursors.DictCursor
    cfg["connect_timeout"] = 30
    cfg["read_timeout"] = 600
    cfg["write_timeout"] = 600
    return pymysql.connect(**cfg)


def salvar_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def novo_id(legacy_id, tipo_planilha):
    tipo_part = _sanitize_id_part(tipo_planilha or "semtipo")
    return f"{legacy_id}_{tipo_part}"[:150]


def garantir_schema(cur, aplicar):
    comandos = [
        """
        ALTER TABLE locatarios
        ADD COLUMN IF NOT EXISTS id_legacy varchar(150) DEFAULT NULL AFTER id
        """,
        """
        ALTER TABLE locatarios
        ADD KEY IF NOT EXISTS idx_locatarios_id_legacy (id_legacy)
        """,
        """
        ALTER TABLE locatarios
        ADD KEY IF NOT EXISTS idx_locatarios_legacy_tipo (id_legacy, tipo_planilha)
        """,
        """
        ALTER TABLE apolices_geradas
        MODIFY COLUMN locatario_id varchar(150) DEFAULT NULL
        """,
    ]
    if not aplicar:
        return comandos
    for sql in comandos:
        cur.execute(sql)
    return comandos


def coluna_existe(cur, tabela, coluna):
    cur.execute(
        """
        SELECT COUNT(*) AS total
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (DB_CONFIG["database"], tabela, coluna),
    )
    row = cur.fetchone()
    return bool(row["total"])


def migrar(aplicar=False):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    conn = conectar_dict()
    resumo = {
        "executado_em": datetime.now().isoformat(),
        "modo": "apply" if aplicar else "dry_run",
        "schema_sql": [],
        "rows_total": 0,
        "rows_to_update": 0,
        "apolices_to_update": 0,
        "already_v2": 0,
        "conflicts": [],
        "samples": [],
    }

    try:
        with conn.cursor() as cur:
            resumo["schema_sql"] = garantir_schema(cur, aplicar)
            has_id_legacy = coluna_existe(cur, "locatarios", "id_legacy")

            if has_id_legacy:
                cur.execute(
                    """
                    SELECT id, id_legacy, tipo_planilha
                    FROM locatarios
                    ORDER BY id
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT id, NULL AS id_legacy, tipo_planilha
                    FROM locatarios
                    ORDER BY id
                    """
                )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT id, locatario_id
                FROM apolices_geradas
                WHERE locatario_id IS NOT NULL AND locatario_id <> ''
                """
            )
            apolices = cur.fetchall()

            apolices_por_loc = {}
            for row in apolices:
                apolices_por_loc.setdefault(row["locatario_id"], []).append(row["id"])

            all_ids = {row["id"] for row in rows}
            updates = []

            for row in rows:
                legacy = row.get("id_legacy") or row["id"]
                target = novo_id(legacy, row.get("tipo_planilha"))
                resumo["rows_total"] += 1
                if row["id"] == target and row.get("id_legacy") == legacy:
                    resumo["already_v2"] += 1
                    continue
                if target != row["id"] and target in all_ids:
                    resumo["conflicts"].append({
                        "current_id": row["id"],
                        "legacy_id": legacy,
                        "target_id": target,
                        "tipo_planilha": row.get("tipo_planilha"),
                    })
                    continue
                updates.append({
                    "current_id": row["id"],
                    "legacy_id": legacy,
                    "target_id": target,
                    "tipo_planilha": row.get("tipo_planilha"),
                    "apolices": apolices_por_loc.get(row["id"], []),
                })

            resumo["rows_to_update"] = len(updates)
            resumo["apolices_to_update"] = sum(len(item["apolices"]) for item in updates)
            resumo["samples"] = updates[:20]

            backup_loc = BACKUP_DIR / f"migracao_id_v2_{timestamp}_locatarios_before.json"
            backup_ap = BACKUP_DIR / f"migracao_id_v2_{timestamp}_apolices_before.json"
            salvar_json(backup_loc, rows)
            salvar_json(backup_ap, apolices)

            if aplicar:
                for idx, item in enumerate(updates, start=1):
                    cur.execute(
                        """
                        UPDATE locatarios
                        SET id_legacy = %s, id = %s
                        WHERE id = %s
                        """,
                        (item["legacy_id"], item["target_id"], item["current_id"]),
                    )
                    if item["apolices"]:
                        cur.execute(
                            """
                            UPDATE apolices_geradas
                            SET locatario_id = %s
                            WHERE locatario_id = %s
                            """,
                            (item["target_id"], item["current_id"]),
                        )
                    if idx % COMMIT_BATCH == 0:
                        conn.commit()
                conn.commit()

            salvar_json(AUDIT_DIR / f"migracao_id_v2_{timestamp}_resultado.json", resumo)
            return resumo
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Aplica a migração do id_v2 no banco.")
    args = parser.parse_args()

    resultado = migrar(aplicar=args.apply)
    print({
        "modo": resultado["modo"],
        "rows_total": resultado["rows_total"],
        "rows_to_update": resultado["rows_to_update"],
        "apolices_to_update": resultado["apolices_to_update"],
        "already_v2": resultado["already_v2"],
        "conflicts": len(resultado["conflicts"]),
    })
