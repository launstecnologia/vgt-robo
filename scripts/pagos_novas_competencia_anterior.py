#!/usr/bin/env python3
"""
Executa:
- pagos do 1º dia do mês atual até hoje
- novas locações do 1º dia do mês atual até hoje
- envia ambos ao banco com competência forçada para o mês anterior

Uso:
  python3 pagos_novas_competencia_anterior.py
  python3 pagos_novas_competencia_anterior.py --nome "BONS NEGOCIOS"
"""

import argparse
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def formatar_data(dt):
    return dt.strftime("%d/%m/%Y")


def periodo_mes_atual_ate_hoje():
    hoje = date.today()
    return hoje.replace(day=1), hoje


def competencia_mes_anterior():
    hoje = date.today()
    primeiro_mes_atual = hoje.replace(day=1)
    ultimo_mes_anterior = primeiro_mes_atual - timedelta(days=1)
    return ultimo_mes_anterior.strftime("%Y-%m")


def rodar(cmd, env=None):
    print(f"Executando: {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=PROJECT_ROOT, env=env)


def argumentos_nomes(nomes):
    args = []
    for nome in nomes:
        args.extend(["--nome", nome])
    return args


def main():
    parser = argparse.ArgumentParser(
        description="Roda pagos e novas locações do mês atual, enviando ao banco com competência do mês anterior."
    )
    parser.add_argument("nomes", nargs="*", help="Filtrar por nome da imobiliária")
    parser.add_argument("--nome", "-n", dest="nome_arg", action="append", help="Adicionar nome para filtrar")
    args = parser.parse_args()

    nomes_busca = list(args.nomes) if args.nomes else []
    if args.nome_arg:
        nomes_busca.extend(args.nome_arg)

    data_inicio, data_fim = periodo_mes_atual_ate_hoje()
    competencia = competencia_mes_anterior()
    inicio = formatar_data(data_inicio)
    fim = formatar_data(data_fim)

    env = os.environ.copy()
    env["ROBO_COMPETENCIA_FIXA"] = competencia
    env["ROBO_COMPETENCIA_FORCADA"] = competencia

    nomes_args = argumentos_nomes(nomes_busca)
    comandos = [
        [sys.executable, str(PROJECT_ROOT / "main_pagos.py"), *nomes_args, "--data-inicio", inicio, "--data-fim", fim],
        [sys.executable, str(PROJECT_ROOT / "main_novas_locacoes.py"), *nomes_args, "--data-inicio", inicio, "--data-fim", fim],
        [sys.executable, str(PROJECT_ROOT / "envioapi.py")],
        [sys.executable, str(PROJECT_ROOT / "envioapi_novas_locacoes.py")],
    ]

    print(f"Período de busca: {inicio} até {fim}")
    print(f"Competência forçada para envio: {competencia}")
    if nomes_busca:
        print(f"Filtro de imobiliárias: {', '.join(nomes_busca)}")

    for cmd in comandos:
        code = rodar(cmd, env=env)
        if code != 0:
            raise SystemExit(code)


if __name__ == "__main__":
    main()
