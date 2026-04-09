# =========================
# Credenciais do banco (mesmo config.php do projeto apólice)
# =========================
# Use variáveis de ambiente para não expor senha em repositório.

import os
from .segredos_loader import carregar_segredos_globais


SEGREDOS_GLOBAIS = carregar_segredos_globais()

DB_CONFIG = {
    "host": os.getenv("ROBO_DB_HOST", SEGREDOS_GLOBAIS.get("db_host", "186.209.113.149")),
    "user": os.getenv("ROBO_DB_USER", SEGREDOS_GLOBAIS.get("db_user", "launs_apolice")),
    "password": os.getenv("ROBO_DB_PASSWORD", SEGREDOS_GLOBAIS.get("db_password", "")),
    "database": os.getenv("ROBO_DB_DATABASE", SEGREDOS_GLOBAIS.get("db_database", "launs_apolice")),
    "charset": "utf8mb4",
}
