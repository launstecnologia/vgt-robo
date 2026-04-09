#!/usr/bin/env python3
"""
Robô MBROKERS — Executa apenas para a imobiliária MBROKERS.

Uso:
  python main_mbrokers.py
  python main_mbrokers.py --data-inicio 01/01/2026 --data-fim 31/01/2026
"""

import argparse

from app.robo_core import executar, IMOBILIARIAS_EVENTOS


NOME_ALVO = "MBROKERS"


def main():
    parser = argparse.ArgumentParser(
        description="Robô KSI — Executa apenas para MBROKERS (todos os tipos)"
    )
    parser.add_argument("--data-inicio", help="Data início (dd/mm/yyyy)")
    parser.add_argument("--data-fim", help="Data fim (dd/mm/yyyy)")
    args = parser.parse_args()

    imobiliarias = [
        imob
        for imob in IMOBILIARIAS_EVENTOS
        if (imob.get("nome") or "").strip().upper() == NOME_ALVO
    ]

    if not imobiliarias:
        print("MBROKERS não encontrada em imobiliarias_eventos.py")
        raise SystemExit(1)

    print("Executando robô somente para MBROKERS")
    executar(None, imobiliarias, data_inicio=args.data_inicio, data_fim=args.data_fim)


if __name__ == "__main__":
    main()
