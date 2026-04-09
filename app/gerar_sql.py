# =========================
# Gerar JSON de locatários a partir dos CSV em downloads/
# =========================
# Lê arquivos que seguem o padrão do robô, mapeia colunas (igual LocatariosImportService PHP),
# grava storage/robo/locatarios_gerados.json para o insert_banco.py consumir.

import os
import re
import json
import glob
import unicodedata
import csv
import zipfile
import xml.etree.ElementTree as ET
from calendar import monthrange
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOWNLOAD_DIR = os.path.join(PROJECT_ROOT, "downloads")
DOWNLOADS_PROCESSADOS_DIR = os.path.join(PROJECT_ROOT, "downloads_processados")
DOWNLOADS_ENTRADA = {
    "pagos": os.path.join(PROJECT_ROOT, "downloads_pagos"),
    "nao_pagos": os.path.join(PROJECT_ROOT, "downloads_nao_pagos"),
    "novas_locacoes": os.path.join(PROJECT_ROOT, "downloads_novas_locacoes"),
}
OUTPUT_JSON = os.path.join(PROJECT_ROOT, "data", "locatarios_gerados.json")

# Competência forçada: use para enviar todos os arquivos para um mês específico.
# Ex.: "2026-03" para março. Por padrão fica desabilitada.
COMPETENCIA_FORCADA = os.getenv("ROBO_COMPETENCIA_FORCADA") or None
# Caminho do seguro_map.json usado pelo PHP (storage/data/seguro_map.json)
SEGURO_MAP_PATH = os.path.abspath(
    os.path.join(PROJECT_ROOT, os.pardir, "storage", "data", "seguro_map.json")
)

from .imobiliarias_eventos import IMOBILIARIAS_EVENTOS

# Padrão: eventos_totalizador_<slug>_pagos|nao_pagos_YYYY-MM-DD_HH-MM.csv
# (.+?) não-guloso: evita que o slug engula "_nao" em *_nao_pagos_*, senão o grupo 2 vira só "pagos".
PATTERN_COM_TIPO = re.compile(
    r"^eventos_totalizador_(.+?)_(nao_pagos|pagos)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(csv|xls|xlsx)$",
    re.IGNORECASE,
)
PATTERN_NOVAS_LOCACOES = re.compile(
    r"^novas_locacoes_(.+)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(csv|xls|xlsx)$",
    re.IGNORECASE,
)
PATTERN_SEM_TIPO = re.compile(
    r"^eventos_totalizador_(.+)_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}\.(csv|xls|xlsx)$",
    re.IGNORECASE,
)

# Aliases de cabeçalho (igual PHP LocatariosImportService)
HEADER_ALIASES = {
    "imovel": "numero_imovel",
    "end_imo": "segurado_endereco",
    "ctr": "contrato",
    "locatario": "locatario_raw",
    "vlr_final": "credito_s_multa",
    "dt_loc_efet": "aluguel_vencimento",
    "inquilino_nome": "segurado_nome",
    "inquilino_pessoa": "segurado_tipo",
    "inquilino_doc": "segurado_cpf_cnpj",
    "cpf_cnpj": "segurado_cpf_cnpj",
    "cpf": "segurado_cpf_cnpj",
    "cnpj": "segurado_cpf_cnpj",
    "cidade": "segurado_cidade",
    "estado": "segurado_uf",
    "bairro": "segurado_bairro",
    "cep": "segurado_cep",
    "endereco": "segurado_endereco",
    "numero": "segurado_numero",
    "complemento": "segurado_complemento",
    "comple": "segurado_complemento",
    "compl": "segurado_complemento",
    "unidade": "segurado_unidade",
    "endereco_risco": "risco_endereco",
    "numero_risco": "risco_numero",
    "bairro_risco": "risco_bairro",
    "cep_risco": "risco_cep",
    "cidade_risco": "risco_cidade",
    "uf_risco": "risco_uf",
    "complemento_risco": "risco_complemento",
    "unidade_risco": "risco_unidade",
    "questionario_risco": "risco_questionario",
    "imovel_codigo": "id",
    "contrato": "contrato",
    "credito_s_multa": "credito_s_multa",
    "incendio": "coberturas_incendio",
    "incendio_conteudo": "coberturas_incendio_conteudo",
    "vendaval": "coberturas_vendaval",
    "perda_aluguel": "coberturas_perda_aluguel",
    "danos_eletricos": "coberturas_danos_eletricos",
    "responsabilidade_civil": "coberturas_responsabilidade_civil",
    "aluguel_vencimento": "aluguel_vencimento",
    "inquilino_email": "segurado_email",
    "inquilino_telefone": "segurado_telefone",
    "inquilino_celular": "segurado_celular",
    "inquilino_celular_2": "segurado_celular_2",
    "proprietario_nome": "coberturas_proprietario_nome",
    "proprietario_email": "coberturas_proprietario_email",
    "proprietario_celular": "coberturas_proprietario_celular",
    "proprietario_celular_2": "coberturas_proprietario_telefone",
    "atendentes": "coberturas_atendentes",
    "fianca": "coberturas_fianca",
    "captacao_usuario": "coberturas_captacao_usuario",
    "captacao_cadastro": "coberturas_captacao_cadastro",
    "vlraluguel": "coberturas_vlr_aluguel",
    "vlr_aluguel": "coberturas_vlr_aluguel",
    "ctrparcrectot": "coberturas_tx_ctr",
    "taxa_adm_parc": "coberturas_taxa_adm_parc",
    "taxa_adm_ctr": "coberturas_taxa_cobrada",
    "locacao_efetiva_data": "coberturas_locacao_efetiva_data",
    "colaborador_de_contato": "coberturas_colaborador_de_contato",
    "empresa_fiscal": "coberturas_mod_gar",
}

REQUIRED_FIELDS = [
    "segurado_nome",
    "segurado_tipo",
    "segurado_cpf_cnpj",
    "segurado_endereco",
    "segurado_bairro",
    "segurado_cep",
    "segurado_cidade",
    "segurado_uf",
]

RISCO_FALLBACK = {
    "risco_endereco": "segurado_endereco",
    "risco_numero": "segurado_numero",
    "risco_complemento": "segurado_complemento",
    "risco_unidade": "segurado_unidade",
    "risco_bairro": "segurado_bairro",
    "risco_cep": "segurado_cep",
    "risco_cidade": "segurado_cidade",
    "risco_uf": "segurado_uf",
}


def normalizar_slug(s):
    s = (s or "").lower().strip()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def normalizar_data_vencimento(val):
    """
    Normaliza string de data (DD/MM/YYYY ou YYYY-MM-DD) para YYYY-MM-DD.
    Retorna None se inválido ou vazio.
    """
    if not val or not str(val).strip():
        return None
    s = str(val).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _sanitize_id_part(value, default="n"):
    s = (value or "").strip()
    if not s:
        s = default
    s = re.sub(r"[^a-zA-Z0-9\-_]", "_", s)
    return s


def gerar_id_locatario_legacy(imobiliaria_id, contrato, numero_imovel, competencia, aluguel_vencimento):
    """
    Gera id sintético alinhado com o PHP:
    imobiliaria_id_contrato_numero_imovel_competencia_vencimento
    - Permite duas parcelas no mesmo mês (mesma competência), diferenciadas pela data de vencimento.
    """
    contrato_part = _sanitize_id_part(contrato)
    numero_part = _sanitize_id_part(numero_imovel)
    comp = competencia or ""
    if not re.match(r"^\d{4}-\d{2}$", comp):
        comp = "0000-00"

    venc_norm = ""
    if aluguel_vencimento:
        data_norm = normalizar_data_vencimento(aluguel_vencimento)
        if data_norm:
            venc_norm = data_norm.replace("-", "")
    if not venc_norm:
        venc_norm = "semvenc"

    return f"{imobiliaria_id}_{contrato_part}_{numero_part}_{comp}_{venc_norm}"[:100]


def gerar_id_locatario(imobiliaria_id, contrato, numero_imovel, competencia, aluguel_vencimento, tipo_planilha=None):
    """
    Gera o novo id do locatário incluindo o tipo da planilha.
    Isso evita colisão entre nao_pagos e novos_cadastros quando ambos
    têm o mesmo contrato/imóvel/competência e vencimento vazio.
    """
    legacy = gerar_id_locatario_legacy(
        imobiliaria_id,
        contrato,
        numero_imovel,
        competencia,
        aluguel_vencimento,
    )
    tipo_part = _sanitize_id_part(tipo_planilha or "semtipo")
    return f"{legacy}_{tipo_part}"[:150]


def parse_decimal_br(value):
    """
    Versão Python do parseDecimalBr do PHP:
    - aceita formatos tipo '1.234,56', '1234,56', '1234.56', 'R$ 1.234,56' etc.
    - retorna float ou None.
    """
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    # Remove símbolo de moeda e espaços
    v = v.replace("R$", "").replace(" ", "")
    # Se tem vírgula e ponto, decidir qual é decimal
    if "," in v and "." in v:
        if v.rfind(",") > v.rfind("."):
            # Formato brasileiro: 1.234,56 -> 1234.56
            v = v.replace(".", "")
            v = v.replace(",", ".")
        else:
            # Formato americano com vírgula de milhar: 1,234.56 -> 1234.56
            v = v.replace(",", "")
    elif "," in v:
        # Apenas vírgula: tratar como decimal brasileiro
        v = v.replace(".", "")
        v = v.replace(",", ".")
    # Agora deve ser algo como 1234.56
    try:
        return float(v)
    except ValueError:
        return None


def load_seguro_map():
    """
    Carrega o mesmo JSON usado pelo PHP (SeguroJsonRepository->load()).
    Se não existir ou estiver inválido, retorna {}.
    """
    if not os.path.isfile(SEGURO_MAP_PATH):
        return {}
    try:
        with open(SEGURO_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def fill_coberturas_from_map(data, seguro_map):
    """
    Replica a lógica do gerar_apolice.php:
    - Usa credito_s_multa como prêmio;
    - Busca linha em seguro_map pelo valor (chave exata ou aproximação);
    - Preenche coberturas_* faltantes (incendio, conteudo, vendaval, perda_aluguel,
      danos_eletricos, responsabilidade_civil) quando houver mapeamento não ambíguo.
    """
    if not seguro_map:
        return data
    premio_raw = (data.get("credito_s_multa") or "").strip()
    if not premio_raw:
        return data
    premio = parse_decimal_br(premio_raw)
    if premio is None or premio < 0:
        return data

    premio_rounded = round(premio, 2)
    key_dot = f"{premio_rounded:.2f}"          # 1234.56
    key_comma = key_dot.replace(".", ",")      # 1234,56

    row = seguro_map.get(key_dot) or seguro_map.get(key_comma)

    if row is None:
        # Aproximação por diferença pequena, igual ao PHP
        for map_key, map_row in seguro_map.items():
            if not isinstance(map_row, dict) or map_row.get("ambiguous"):
                continue
            # No PHP: parseDecimalBr(str_replace('.', ',', $mapKey))
            map_key_br = str(map_key).replace(".", ",")
            key_num = parse_decimal_br(map_key_br)
            if key_num is None:
                continue
            if abs(round(key_num, 2) - premio_rounded) < 0.005:
                row = map_row
                break

    if not isinstance(row, dict) or row.get("ambiguous"):
        return data

    base_keys = [
        "incendio",
        "incendio_conteudo",
        "vendaval",
        "perda_aluguel",
        "danos_eletricos",
        "responsabilidade_civil",
    ]
    for base in base_keys:
        val = row.get(base)
        if val is None or str(val).strip() == "":
            continue
        dest_key = f"coberturas_{base}"
        if not (data.get(dest_key) or "").strip():
            data[dest_key] = str(val)

    return data


def slug_nome(s):
    s = str(s).strip()
    s = re.sub(r'[/\\:*?"<>|]', "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:80] if s else "arquivo"


def id_imobiliaria_por_slug(slug):
    """
    Resolve id_imobiliaria a partir do slug do arquivo, usando comparação normalizada
    (sem acentos, caixa e underscores extras), igual ao envioapi.py.
    """
    slug_limpo = re.sub(r"_(pagos|nao_pagos)$", "", slug, flags=re.IGNORECASE).strip()
    slug_norm = normalizar_slug(slug_limpo)
    for imob in IMOBILIARIAS_EVENTOS:
        nome = imob.get("nome", "")
        nome_norm = normalizar_slug(slug_nome(nome))
        if nome_norm == slug_norm or nome_norm.replace("_", "") == slug_norm.replace("_", ""):
            return imob.get("id_imobiliaria"), imob.get("nome")
        if slug_norm.startswith(nome_norm + "_") or slug_norm.replace("_", "").startswith(nome_norm.replace("_", "")):
            return imob.get("id_imobiliaria"), imob.get("nome")
    return None, None


def extrair_metadados(nome_arquivo, tipo_padrao=None):
    m = PATTERN_COM_TIPO.match(nome_arquivo)
    if m:
        slug, tipo, data_str = m.group(1).strip(), m.group(2).lower(), m.group(3)
        return slug, data_str[:7], tipo
    m = PATTERN_NOVAS_LOCACOES.match(nome_arquivo)
    if m:
        slug, data_str = m.group(1).strip(), m.group(2)
        return slug, data_str[:7], "novos_cadastros"
    m = PATTERN_SEM_TIPO.match(nome_arquivo)
    if m:
        slug, data_str = m.group(1).strip(), m.group(2)
        return slug, data_str[:7], tipo_padrao or "pagos"
    return None, None, None


def listar_planilhas_entrada():
    arquivos = []
    for tipo_planilha, pasta in DOWNLOADS_ENTRADA.items():
        if not os.path.isdir(pasta):
            continue
        for ext in ("*.csv", "*.xls", "*.xlsx"):
            for caminho in glob.glob(os.path.join(pasta, "**", ext), recursive=True):
                arquivos.append((caminho, tipo_planilha))
    for ext in ("*.csv", "*.xls", "*.xlsx"):
        for caminho in glob.glob(os.path.join(DOWNLOAD_DIR, "**", ext), recursive=True):
            caminho_abs = os.path.abspath(caminho)
            if caminho_abs.startswith(os.path.abspath(DOWNLOADS_PROCESSADOS_DIR) + os.sep):
                continue
            arquivos.append((caminho, None))
    vistos = set()
    unicos = []
    for caminho, tipo_planilha in arquivos:
        chave = os.path.abspath(caminho)
        if chave in vistos:
            continue
        vistos.add(chave)
        unicos.append((caminho, tipo_planilha))
    return unicos


def normalizar_header(col):
    col = unicodedata.normalize("NFD", (col or "").strip().lower())
    col = "".join(c for c in col if unicodedata.category(c) != "Mn")
    col = col.replace(" ", "_").replace("-", "_")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col.strip("_")


def preencher_risco_fallback(data):
    for risk, fallback in RISCO_FALLBACK.items():
        if not (data.get(risk) or "").strip() and (data.get(fallback) or "").strip():
            data[risk] = data.get(fallback, "")
    return data


def validar_row(data):
    missing = []
    for f in REQUIRED_FIELDS:
        if not (data.get(f) or "").strip():
            missing.append(f)
    for risk, fallback in RISCO_FALLBACK.items():
        if not (data.get(risk) or "").strip() and not (data.get(fallback) or "").strip():
            missing.append(risk)
    return missing


def vigencia_mes(competencia):
    try:
        ano, mes = int(competencia[:4]), int(competencia[5:7])
        ultimo = monthrange(ano, mes)[1]
        return f"{ano}-{mes:02d}-01", f"{ano}-{mes:02d}-{ultimo}"
    except (ValueError, IndexError):
        return None, None


def normalizar_layout_compacto(data):
    raw_locatario = (data.get("locatario_raw") or "").strip()
    if raw_locatario:
        if not data.get("segurado_cpf_cnpj"):
            m_doc = re.search(r"(CPF|CNPJ):\s*([\d./-]+)", raw_locatario, re.IGNORECASE)
            if m_doc:
                data["segurado_cpf_cnpj"] = m_doc.group(2).strip()
        if not data.get("segurado_tipo"):
            digits = re.sub(r"\D", "", data.get("segurado_cpf_cnpj") or "")
            data["segurado_tipo"] = "PJ" if len(digits) == 14 else "PF"
        if not data.get("segurado_nome"):
            nome = re.split(r"(CPF|CNPJ):", raw_locatario, maxsplit=1, flags=re.IGNORECASE)[0]
            nome = re.split(r"\s{2,}", nome, maxsplit=1)[0].strip()
            data["segurado_nome"] = nome

    endereco_bruto = (data.get("segurado_endereco") or "").strip()
    if endereco_bruto:
        partes = [p.strip() for p in endereco_bruto.split(" - ") if p.strip()]
        if partes:
            data["segurado_endereco"] = partes[0]
        if not data.get("segurado_cep"):
            for parte in partes[1:]:
                if re.fullmatch(r"\d{2}\.\d{3}-\d{3}", parte) or re.fullmatch(r"\d{8}", parte):
                    data["segurado_cep"] = parte
                    break
        if not data.get("segurado_cidade") or not data.get("segurado_uf"):
            for parte in reversed(partes):
                m_cidade = re.fullmatch(r"(.+?)/([A-Z]{2})", parte)
                if m_cidade:
                    data["segurado_cidade"] = data.get("segurado_cidade") or m_cidade.group(1).strip()
                    data["segurado_uf"] = data.get("segurado_uf") or m_cidade.group(2).strip()
                    break
        if not data.get("segurado_bairro"):
            cidade_uf = f"{data.get('segurado_cidade', '')}/{data.get('segurado_uf', '')}".strip("/")
            for parte in partes[1:]:
                if parte == data.get("segurado_cep") or parte == cidade_uf:
                    continue
                data["segurado_bairro"] = parte
                break

    return preencher_risco_fallback(data)


def map_to_locatario(data, row_num):
    coberturas_base = {
        "incendio": (data.get("coberturas_incendio") or "").strip(),
        "incendio_conteudo": (data.get("coberturas_incendio_conteudo") or "").strip(),
        "vendaval": (data.get("coberturas_vendaval") or "").strip(),
        "perda_aluguel": (data.get("coberturas_perda_aluguel") or "").strip(),
        "danos_eletricos": (data.get("coberturas_danos_eletricos") or "").strip(),
        "responsabilidade_civil": (data.get("coberturas_responsabilidade_civil") or "").strip(),
    }
    coberturas_ksi = {
        "proprietario_nome": (data.get("coberturas_proprietario_nome") or "").strip(),
        "proprietario_email": (data.get("coberturas_proprietario_email") or "").strip(),
        "proprietario_celular": (data.get("coberturas_proprietario_celular") or data.get("coberturas_proprietario_telefone") or "").strip(),
        "atendentes": (data.get("coberturas_atendentes") or "").strip(),
        "fianca": (data.get("coberturas_fianca") or "").strip(),
        "captacao_usuario": (data.get("coberturas_captacao_usuario") or "").strip(),
        "captacao_cadastro": (data.get("coberturas_captacao_cadastro") or "").strip(),
        "vlr_aluguel": (data.get("coberturas_vlr_aluguel") or "").strip(),
        "tx_ctr": (data.get("coberturas_tx_ctr") or "").strip(),
        "taxa_adm_parc": (data.get("coberturas_taxa_adm_parc") or "").strip(),
        "taxa_cobrada": (data.get("coberturas_taxa_cobrada") or "").strip(),
        "locacao_efetiva_data": (data.get("coberturas_locacao_efetiva_data") or "").strip(),
        "colaborador_de_contato": (data.get("coberturas_colaborador_de_contato") or "").strip(),
        "mod_gar": (data.get("coberturas_mod_gar") or "").strip(),
    }
    coberturas = {**coberturas_base, **{k: v for k, v in coberturas_ksi.items() if v}}

    credito = (data.get("credito_s_multa") or data.get("coberturas_vlr_aluguel") or "").strip()

    return {
        "id": (data.get("id") or data.get("segurado_cpf_cnpj") or f"row_{row_num}").strip()[:100],
        "numero_imovel": (data.get("numero_imovel") or data.get("id") or "").strip()[:50],
        "contrato": (data.get("contrato") or "").strip()[:50],
        "credito_s_multa": credito[:50] if credito else "",
        "segurado": {
            "nome": (data.get("segurado_nome") or "").strip(),
            "tipo": (data.get("segurado_tipo") or "").strip(),
            "cpf_cnpj": (data.get("segurado_cpf_cnpj") or "").strip(),
            "endereco": (data.get("segurado_endereco") or "").strip(),
            "numero": (data.get("segurado_numero") or "").strip(),
            "unidade": (data.get("segurado_unidade") or "").strip(),
            "complemento": (data.get("segurado_complemento") or "").strip(),
            "bairro": (data.get("segurado_bairro") or "").strip(),
            "cep": (data.get("segurado_cep") or "").strip(),
            "cidade": (data.get("segurado_cidade") or "").strip(),
            "uf": (data.get("segurado_uf") or "").strip(),
            "email": (data.get("segurado_email") or "").strip(),
            "telefone": (data.get("segurado_telefone") or data.get("segurado_celular") or "").strip(),
            "celular": (data.get("segurado_celular") or "").strip(),
            "celular_2": (data.get("segurado_celular_2") or "").strip(),
        },
        "risco": {
            "endereco": (data.get("risco_endereco") or "").strip(),
            "numero": (data.get("risco_numero") or "").strip(),
            "complemento": (data.get("risco_complemento") or "").strip(),
            "unidade": (data.get("risco_unidade") or "").strip(),
            "bairro": (data.get("risco_bairro") or "").strip(),
            "cep": (data.get("risco_cep") or "").strip(),
            "cidade": (data.get("risco_cidade") or "").strip(),
            "uf": (data.get("risco_uf") or "").strip(),
            "questionario": (data.get("risco_questionario") or "NAO").strip(),
        },
        "coberturas": coberturas,
    }


def detectar_delimitador(caminho):
    """Detecta delimitador como no PHP LocatariosImportService (; , ou tab)."""
    try:
        with open(caminho, "rb") as f:
            line1 = f.readline()
            line2 = f.readline()
        sample = (line1 or b"") + (line2 or b"")
        best = ";"
        best_count = 0
        for delim in (b";", b",", b"\t"):
            if sample.count(delim) > best_count:
                best_count = sample.count(delim)
                best = delim.decode("ascii", errors="replace")
        return best
    except Exception:
        return ";"


def ler_csv(caminho):
    """
    Lê CSV. Tenta UTF-8 primeiro; se falhar (arquivo em Windows-1252), usa cp1252
    para preservar acentos (á, ã, í, etc.), igual ao PHP LocatariosImportService.
    """
    delim = detectar_delimitador(caminho)
    try:
        with open(caminho, "r", encoding="utf-8", errors="strict") as f:
            reader = csv.DictReader(f, delimiter=delim)
            rows = list(reader)
    except UnicodeDecodeError:
        with open(caminho, "r", encoding="cp1252", errors="replace") as f:
            reader = csv.DictReader(f, delimiter=delim)
            rows = list(reader)
    if not rows:
        return [], {}
    header_map = {}
    for k in rows[0].keys():
        norm = normalizar_header(k)
        canonical = HEADER_ALIASES.get(norm, norm)
        header_map[k] = canonical
    return rows, header_map


def _xlsx_texto_celula(cell, shared_strings, ns):
    tipo = cell.get("t")
    if tipo == "inlineStr":
        return "".join((node.text or "") for node in cell.iter() if node.tag.endswith("}t"))
    v = cell.find("a:v", ns)
    if v is None or v.text is None:
        return ""
    if tipo == "s":
        try:
            return shared_strings[int(v.text)]
        except Exception:
            return ""
    return v.text


def _xlsx_col_index(ref):
    letras = "".join(ch for ch in (ref or "") if ch.isalpha()).upper()
    idx = 0
    for ch in letras:
        idx = idx * 26 + (ord(ch) - 64)
    return max(idx - 1, 0)


def _xlsx_primeira_planilha_path(zf):
    """
    Resolve o caminho da primeira planilha do XLSX de forma robusta.
    Evita depender fixamente de xl/worksheets/sheet1.xml.
    """
    default = "xl/worksheets/sheet1.xml"
    try:
        nomes = set(zf.namelist())
        if default in nomes:
            return default

        # Fallback simples: primeira sheet*.xml encontrada.
        sheets = sorted(
            n for n in nomes
            if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")
        )
        if sheets:
            return sheets[0]
    except Exception:
        pass
    return default


def ler_xlsx(caminho):
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(caminho) as zf:
        shared_strings = []
        if "xl/sharedStrings.xml" in zf.namelist():
            ss_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in ss_root.findall("a:si", ns):
                shared_strings.append("".join((node.text or "") for node in si.iter() if node.tag.endswith("}t")))

        sheet_path = _xlsx_primeira_planilha_path(zf)
        root = ET.fromstring(zf.read(sheet_path))
        sheet_data = root.find("a:sheetData", ns)
        if sheet_data is None:
            return [], {}

        linhas = []
        for row in sheet_data.findall("a:row", ns):
            valores = {}
            for cell in row.findall("a:c", ns):
                idx = _xlsx_col_index(cell.get("r", ""))
                valores[idx] = _xlsx_texto_celula(cell, shared_strings, ns)
            if not valores:
                continue
            max_idx = max(valores.keys())
            linhas.append([valores.get(i, "") for i in range(max_idx + 1)])

    if not linhas:
        return [], {}

    max_scan = min(20, len(linhas))
    melhor_idx = 0
    melhor_score = -1
    for idx, cols in enumerate(linhas[:max_scan]):
        score = 0
        for valor in cols:
            norm = normalizar_header(valor)
            canonical = HEADER_ALIASES.get(norm, norm)
            if canonical in REQUIRED_FIELDS or canonical in ("numero_imovel", "contrato", "credito_s_multa", "aluguel_vencimento", "locatario_raw"):
                score += 1
        if score > melhor_score:
            melhor_score = score
            melhor_idx = idx

    header_map = {}
    for pos, valor in enumerate(linhas[melhor_idx]):
        norm = normalizar_header(valor)
        if not norm:
            continue
        header_map[pos] = HEADER_ALIASES.get(norm, norm)

    rows = []
    for cols in linhas[melhor_idx + 1:]:
        row = {}
        for pos, canonical in header_map.items():
            row[canonical] = (cols[pos] if pos < len(cols) else "").strip()
        if any(v != "" for v in row.values()):
            rows.append(row)
    return rows, header_map


def processar_arquivo(caminho, id_imob, competencia, tipo_planilha, seguro_map):
    ext = os.path.splitext(caminho)[1].lower()
    if ext == ".csv":
        rows, header_map = ler_csv(caminho)
    elif ext == ".xlsx":
        rows, header_map = ler_xlsx(caminho)
    else:
        return []
    if not header_map:
        return []
    vigencia_inicio, vigencia_fim = vigencia_mes(competencia)
    # Um único registro por id (por imobiliária). Em pagos com mesmo id em várias linhas, a última vence.
    by_id = {}
    for i, row in enumerate(rows):
        data = {}
        for col_name, value in row.items():
            canonical = header_map.get(col_name)
            if canonical:
                data[canonical] = (value or "").strip()
            else:
                data[col_name] = (value or "").strip()
        data = normalizar_layout_compacto(data)
        data = fill_coberturas_from_map(data, seguro_map)
        data = preencher_risco_fallback(data)
        loc = map_to_locatario(data, i + 2)
        if tipo_planilha == "pagos":
            data_norm = normalizar_data_vencimento(data.get("aluguel_vencimento"))
            loc["aluguel_vencimento"] = data_norm if data_norm else ""
        else:
            loc["aluguel_vencimento"] = ""
        loc["imobiliaria_id"] = id_imob
        loc["tipo_planilha"] = tipo_planilha
        loc["competencia"] = competencia
        loc["vigencia_inicio"] = vigencia_inicio
        loc["vigencia_fim"] = vigencia_fim
        # Gera id sintético compatível com o PHP (inclui competência e vencimento),
        # permitindo múltiplas parcelas no mesmo mês sem sobrescrever registros antigos.
        legacy_id = gerar_id_locatario_legacy(
            id_imob,
            loc.get("contrato") or "",
            loc.get("numero_imovel") or "",
            competencia,
            loc.get("aluguel_vencimento") or "",
        )
        base_id = gerar_id_locatario(
            id_imob,
            loc.get("contrato") or "",
            loc.get("numero_imovel") or "",
            competencia,
            loc.get("aluguel_vencimento") or "",
            tipo_planilha,
        )
        loc_id = base_id
        if loc_id in by_id:
            # Evita sobrescrever linhas quando contrato/imóvel/vencimento
            # resultam no mesmo id dentro da mesma planilha.
            sufixo = f"_ln{i + 2}"
            loc_id = f"{base_id[: max(1, 150 - len(sufixo))]}{sufixo}"
            contador = 2
            while loc_id in by_id:
                sufixo = f"_ln{i + 2}_{contador}"
                loc_id = f"{base_id[: max(1, 150 - len(sufixo))]}{sufixo}"
                contador += 1
        loc["id"] = loc_id
        loc["id_legacy"] = legacy_id
        by_id[loc["id"]] = loc
    return list(by_id.values())


def main():
    seguro_map = load_seguro_map()
    saida = []
    for caminho, tipo_padrao in listar_planilhas_entrada():
        nome = os.path.basename(caminho)
        slug, competencia, tipo_planilha = extrair_metadados(nome, tipo_padrao=tipo_padrao)
        if tipo_planilha == "novas_locacoes":
            tipo_planilha = "novos_cadastros"
        if not slug or not competencia or tipo_planilha not in ("pagos", "nao_pagos", "novos_cadastros"):
            continue
        if COMPETENCIA_FORCADA:
            competencia = COMPETENCIA_FORCADA
        id_imob, _ = id_imobiliaria_por_slug(slug)
        if id_imob is None:
            continue
        if caminho.lower().endswith((".csv", ".xlsx")):
            locs = processar_arquivo(caminho, id_imob, competencia, tipo_planilha, seguro_map)
        else:
            continue
        saida.append({
            "arquivo": nome,
            "imobiliaria_id": id_imob,
            "competencia": competencia,
            "tipo_planilha": tipo_planilha,
            "locatarios": locs,
        })
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(saida, f, ensure_ascii=False, indent=2)
    return saida
