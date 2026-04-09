#!/usr/bin/env python3
"""
Lista as opções do dropdown de eventos para uma imobiliária.
Uso: python debug_listar_eventos.py
     python debug_listar_eventos.py PEDRO GRANADO
"""
import sys
import time
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from app.imobiliarias_eventos import IMOBILIARIAS_EVENTOS
import logging
from app.robo_core import iniciar_driver, fazer_login_e_ir_para_adm_locacao, navegar_ate_totalizador

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def main():
    import os
    os.chdir(PROJECT_ROOT)
    nome_busca = (sys.argv[1] if len(sys.argv) > 1 else "PEDRO GRANADO").upper()
    imob = next((i for i in IMOBILIARIAS_EVENTOS if nome_busca in (i.get("nome") or "").upper()), None)
    if not imob:
        print(f"Imobiliária '{nome_busca}' não encontrada.")
        sys.exit(1)
    print(f"Conectando em {imob['nome']}...")
    driver = iniciar_driver()
    try:
        driver.get(imob["url"])
        time.sleep(3)
        fazer_login_e_ir_para_adm_locacao(driver, logging.getLogger(), imob)
        navegar_ate_totalizador(driver, logging.getLogger())
        select = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "eventos_tipo"))
        )
        options = select.find_elements(By.TAG_NAME, "option")
        print(f"\n=== Opções no dropdown de eventos ({imob['nome']}) ===\n")
        for i, opt in enumerate(options):
            txt = (opt.text or "").strip()
            if txt:
                print(f"  {i}: {repr(txt)}")
        print(f"\nTotal: {len([o for o in options if (o.text or '').strip()])} opções")
        print(f"\nConfigurado em imobiliarias_eventos.py: {imob.get('eventos')}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
