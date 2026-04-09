import json
import os
from copy import deepcopy


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEGREDOS_DIR = os.path.join(PROJECT_ROOT, "segredos")
SEGREDOS_GLOBAIS_PATH = os.path.join(SEGREDOS_DIR, "robo.local.json")
SEGREDOS_IMOBILIARIAS_PATH = os.path.join(SEGREDOS_DIR, "imobiliarias.local.json")


def _carregar_json(path):
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def carregar_segredos_globais():
    return _carregar_json(SEGREDOS_GLOBAIS_PATH)


def carregar_segredos_imobiliarias():
    data = _carregar_json(SEGREDOS_IMOBILIARIAS_PATH)
    imobs = data.get("imobiliarias")
    return imobs if isinstance(imobs, dict) else {}


def aplicar_segredos_imobiliarias(imobiliarias):
    overrides = carregar_segredos_imobiliarias()
    if not overrides:
        return imobiliarias

    resultado = []
    for imob in imobiliarias:
        novo = deepcopy(imob)
        nome = (imob.get("nome") or "").strip().upper()
        por_nome = overrides.get(nome) or overrides.get(imob.get("nome") or "")
        por_id = overrides.get(str(imob.get("id_imobiliaria")))
        cred = por_id or por_nome
        if isinstance(cred, dict):
            for chave in ("login", "senha", "senha_template"):
                if chave in cred and cred.get(chave):
                    novo[chave] = cred[chave]
        resultado.append(novo)
    return resultado
