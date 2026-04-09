#!/usr/bin/env python3
"""
Robô KSI — Executa os 3 tipos em sequência (Pagos, Não Pagos, Novas locações).
Para rodar apenas um tipo, use:
  python main_pagos.py
  python main_nao_pagos.py
  python main_novas_locacoes.py
"""
import argparse
from app.robo_core import executar, filtrar_por_nome, IMOBILIARIAS_EVENTOS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Robô KSI — Download de planilhas (todos os tipos)")
    parser.add_argument("nomes", nargs="*", help="Filtrar por nome da imobiliária")
    parser.add_argument("--nome", "-n", dest="nome_arg", action="append", help="Adicionar nome para filtrar")
    parser.add_argument("--data-inicio", help="Data início (dd/mm/yyyy, ex: 01/02/2026)")
    parser.add_argument("--data-fim", help="Data fim (dd/mm/yyyy, ex: 28/02/2026)")
    args = parser.parse_args()
    nomes_busca = list(args.nomes) if args.nomes else []
    if args.nome_arg:
        nomes_busca.extend(args.nome_arg)
    imobiliarias = filtrar_por_nome(IMOBILIARIAS_EVENTOS, nomes_busca) if nomes_busca else IMOBILIARIAS_EVENTOS
    if nomes_busca and not imobiliarias:
        print(f"Nenhuma imobiliária encontrada para: {nomes_busca}")
        exit(1)
    executar(None, imobiliarias, data_inicio=args.data_inicio, data_fim=args.data_fim)
