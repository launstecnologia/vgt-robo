#!/usr/bin/env python3
"""
Executa a rotina diária:
- pagos do 1º dia do mês até hoje
- novas locações do 1º dia do mês até hoje
- envio para o banco
"""

import subprocess
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def formatar_data(dt):
    return dt.strftime("%d/%m/%Y")


def periodo_rotina_diaria():
    hoje = date.today()
    return hoje.replace(day=1), hoje


def rodar(cmd):
    print(f"Executando: {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=PROJECT_ROOT)


def main():
    data_inicio, hoje = periodo_rotina_diaria()
    inicio = formatar_data(data_inicio)
    fim = formatar_data(hoje)

    comandos = [
        [sys.executable, str(PROJECT_ROOT / "main_pagos.py"), "--data-inicio", inicio, "--data-fim", fim],
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
