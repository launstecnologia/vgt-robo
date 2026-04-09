#!/usr/bin/env python3
"""
Roda o robô PAGOS apenas para as imobiliárias que falharam na última execução.
Uso: python run_repetir_falhas.py
"""
import json
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESUMO_PATH = PROJECT_ROOT / "logs" / "robo_resumo.json"
IMOBILIAS_FALHAS_PADRAO = ["CONCRETO", "ESTRUTURA", "MBROKERS", "PEDRO GRANADO", "PHERCON"]


def main():
    data_inicio = "01/02/2026"
    data_fim = "28/02/2026"
    falhas = IMOBILIAS_FALHAS_PADRAO

    try:
        with RESUMO_PATH.open("r", encoding="utf-8") as f:
            resumo = json.load(f)
        falhas = [r["imob"] for r in resumo.get("resultados", []) if r.get("status") == "falha"]
        periodo = resumo.get("periodo") or {}
        if periodo:
            data_inicio = periodo.get("data_inicio", data_inicio)
            data_fim = periodo.get("data_fim", data_fim)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    if not falhas:
        print("Nenhuma falha no último resumo. Usando lista padrão.")
        falhas = IMOBILIAS_FALHAS_PADRAO

    print(f"Repetindo PAGOS para {len(falhas)} imobiliária(s): {', '.join(falhas)}")
    print(f"Período: {data_inicio} até {data_fim}")

    cmd = [sys.executable, str(PROJECT_ROOT / "main_pagos.py"), "--data-inicio", data_inicio, "--data-fim", data_fim] + falhas
    raise SystemExit(subprocess.call(cmd, cwd=PROJECT_ROOT))
