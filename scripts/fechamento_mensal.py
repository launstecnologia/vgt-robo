#!/usr/bin/env python3
"""
Executa o fechamento mensal:
- pagos do mês anterior
- não pagos do mês anterior
- novas locações do mês anterior
- envio para o banco
"""

import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def formatar_data(dt):
    return dt.strftime("%d/%m/%Y")


def rodar(cmd):
    print(f"Executando: {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=PROJECT_ROOT)


def periodo_mes_anterior():
    hoje = date.today()
    primeiro_mes_atual = hoje.replace(day=1)
    ultimo_mes_anterior = primeiro_mes_atual - timedelta(days=1)
    primeiro_mes_anterior = ultimo_mes_anterior.replace(day=1)
    return primeiro_mes_anterior, ultimo_mes_anterior


def main():
    data_inicio, data_fim = periodo_mes_anterior()
    inicio = formatar_data(data_inicio)
    fim = formatar_data(data_fim)

    comandos = [
        [sys.executable, str(PROJECT_ROOT / "main_pagos.py"), "--data-inicio", inicio, "--data-fim", fim],
        [sys.executable, str(PROJECT_ROOT / "main_nao_pagos.py"), "--data-inicio", inicio, "--data-fim", fim],
        [sys.executable, str(PROJECT_ROOT / "main_novas_locacoes.py"), "--data-inicio", inicio, "--data-fim", fim],
        [sys.executable, str(PROJECT_ROOT / "envioapi.py")],
        [sys.executable, str(PROJECT_ROOT / "envioapi_novas_locacoes.py")],
    ]

    for cmd in comandos:
        code = rodar(cmd)
        if code != 0:
            raise SystemExit(code)


if __name__ == "__main__":
    main()
