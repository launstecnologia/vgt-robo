#!/usr/bin/env python3
"""
Robô NOVAS LOCAÇÕES — Baixa apenas planilhas de Novas locações (novos cadastros).
Só processa imobiliárias com novas_locacoes=True no imobiliarias_eventos.py.
Uso: python main_novas_locacoes.py [ADDAD] [AGNELLO] ...
     python main_novas_locacoes.py --nome ADDAD
"""
import argparse
from app.robo_core import executar, filtrar_por_nome, IMOBILIARIAS_EVENTOS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Robô NOVAS LOCAÇÕES — Download de planilhas Novas locações")
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
    executar("novas_locacoes", imobiliarias, data_inicio=args.data_inicio, data_fim=args.data_fim)
