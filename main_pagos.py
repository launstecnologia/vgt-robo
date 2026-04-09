#!/usr/bin/env python3
"""
Robô PAGOS — Baixa apenas planilhas de situação Pagos.
Uso: python main_pagos.py [ADDAD] [AGNELLO] ...
     python main_pagos.py --nome ADDAD
"""
import argparse
from app.robo_core import executar, filtrar_por_nome, IMOBILIARIAS_EVENTOS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Robô PAGOS — Download de planilhas Pagos")
    parser.add_argument("nomes", nargs="*", help="Filtrar por nome da imobiliária")
    parser.add_argument("--nome", "-n", dest="nome_arg", action="append", help="Adicionar nome para filtrar")
    parser.add_argument("--data-inicio", help="Data início (dd/mm/yyyy)")
    parser.add_argument("--data-fim", help="Data fim (dd/mm/yyyy)")
    args = parser.parse_args()
    nomes_busca = list(args.nomes) if args.nomes else []
    if args.nome_arg:
        nomes_busca.extend(args.nome_arg)
    imobiliarias = filtrar_por_nome(IMOBILIARIAS_EVENTOS, nomes_busca) if nomes_busca else IMOBILIARIAS_EVENTOS
    if nomes_busca and not imobiliarias:
        print(f"Nenhuma imobiliária encontrada para: {nomes_busca}")
        exit(1)
    executar("pagos", imobiliarias, data_inicio=args.data_inicio, data_fim=args.data_fim)
