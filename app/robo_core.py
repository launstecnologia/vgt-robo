# =========================
# MÓDULO CORE DO ROBÔ KSI
# =========================
# Contém toda a lógica compartilhada. Os scripts main_pagos.py, main_nao_pagos.py
# e main_novas_locacoes.py importam e chamam executar(tipo_planilha, imobiliarias).
# =========================

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import logging
import traceback
import shutil
import csv
import re
from datetime import datetime, timedelta
import os
import calendar
import unicodedata
import requests
from .segredos_loader import carregar_segredos_globais

# =========================
# CONFIGURAÇÕES
# =========================
SEGREDOS_GLOBAIS = carregar_segredos_globais()
BASE_URL = os.getenv("ROBO_BASE_URL", SEGREDOS_GLOBAIS.get("base_url", "https://www.lagoimobiliaria.com.br/kurole-sistema-imobiliario/index.php"))
LOGIN_EMAIL = os.getenv("ROBO_LOGIN_EMAIL", SEGREDOS_GLOBAIS.get("login_email", ""))
DIA_ATUAL = datetime.now().strftime("%d")
LOGIN_SENHA_TEMPLATE = os.getenv("ROBO_LOGIN_SENHA_TEMPLATE", SEGREDOS_GLOBAIS.get("login_senha_template", ""))
LOGIN_SENHA = os.getenv("ROBO_LOGIN_SENHA", "")
TEMPO_ESPERA = 15
ROBO_LOG_ATUAL = None
MAX_TENTATIVAS_IMOBILIARIA = 3
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOWNLOAD_DIR = os.path.join(PROJECT_ROOT, "downloads")
DOWNLOADS_POR_TIPO = {
    "pagos": os.path.join(PROJECT_ROOT, "downloads_pagos"),
    "nao_pagos": os.path.join(PROJECT_ROOT, "downloads_nao_pagos"),
    "novas_locacoes": os.path.join(PROJECT_ROOT, "downloads_novas_locacoes"),
}
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
ERROS_IMOB_DIR = os.path.join(LOGS_DIR, "erros_imobiliarias")
ROBO_RESUMO_PATH = os.path.join(LOGS_DIR, "robo_resumo.json")
API_UPLOAD_URL = os.getenv("ROBO_API_UPLOAD_URL", SEGREDOS_GLOBAIS.get("api_upload_url", "https://apolice.launs.com/api_upload_locatarios.php"))
API_UPLOAD_KEY = os.getenv("ROBO_API_UPLOAD_KEY", SEGREDOS_GLOBAIS.get("api_upload_key", ""))

from .imobiliarias_eventos import IMOBILIARIAS_EVENTOS

# =========================
# LOGGER
# =========================
def nome_log_principal(tipo_planilha):
    if tipo_planilha == "pagos":
        return "geral_pagos.log"
    if tipo_planilha == "nao_pagos":
        return "geral_nao_pagos.log"
    if tipo_planilha == "novas_locacoes":
        return "geral_novas_locacoes.log"
    return "geral_todos.log"

def configurar_logger(tipo_planilha=None):
    dir_logs = os.path.dirname(ROBO_RESUMO_PATH)
    if not os.path.exists(dir_logs):
        os.makedirs(dir_logs)
    global ROBO_LOG_ATUAL
    sufixo = f"_{tipo_planilha}" if tipo_planilha else ""
    nome_arquivo = datetime.now().strftime(f"bot{sufixo}_%Y-%m-%d_%H-%M-%S.log")
    caminho_log = os.path.join(dir_logs, nome_arquivo)
    caminho_log_principal = os.path.join(dir_logs, nome_log_principal(tipo_planilha))
    ROBO_LOG_ATUAL = caminho_log

    logger = logging.getLogger("BOT")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handlers = [
        logging.FileHandler(caminho_log, encoding="utf-8"),
        logging.FileHandler(caminho_log_principal, encoding="utf-8"),
        logging.StreamHandler(),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def pasta_download_tipo(tipo_planilha):
    return DOWNLOADS_POR_TIPO.get(tipo_planilha, DOWNLOAD_DIR)


def pasta_data_execucao(data_referencia=None):
    return datetime.now().strftime("%d-%m")


def nome_pasta_tipo(tipo_planilha):
    mapa = {
        "pagos": "pagos",
        "nao_pagos": "nao-pagos",
        "novas_locacoes": "novos-locatarios",
        "novos_cadastros": "novos-locatarios",
    }
    return mapa.get(tipo_planilha, tipo_planilha or "geral")


def pasta_download_imobiliaria(nome_imob, tipo_planilha, data_referencia=None):
    pasta_imob = slug_nome(nome_imob)
    pasta_data = pasta_data_execucao()
    pasta_tipo = nome_pasta_tipo(tipo_planilha)
    caminho = os.path.join(DOWNLOAD_DIR, pasta_imob, pasta_data, pasta_tipo)
    os.makedirs(caminho, exist_ok=True)
    return caminho

# =========================
# DRIVER
# =========================
def iniciar_driver():
    options = Options()
    options.add_argument("--start-maximized")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    for pasta in DOWNLOADS_POR_TIPO.values():
        os.makedirs(pasta, exist_ok=True)
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)
    chromedriver_local = os.getenv("ROBO_CHROMEDRIVER_PATH", os.path.join(PROJECT_ROOT, "bin", "chromedriver"))
    if os.path.isfile(chromedriver_local):
        service = Service(chromedriver_local)
    else:
        service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

# =========================
# FUNÇÕES BASE (XPATH)
# =========================
def esperar_xpath(driver, xpath):
    return WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.visibility_of_element_located((By.XPATH, xpath))
    )

def clicar_xpath(driver, xpath):
    WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    ).click()

def digitar_xpath(driver, xpath, texto):
    campo = esperar_xpath(driver, xpath)
    campo.clear()
    campo.send_keys(texto)

def clicar_xpath_js(driver, xpath):
    elemento = WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elemento)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", elemento)

def clicar_primeiro_xpath(driver, lista_xpaths, tempo_por_tentativa=5):
    for xpath in lista_xpaths:
        try:
            elemento = WebDriverWait(driver, tempo_por_tentativa).until(
                EC.presence_of_element_located((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elemento)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", elemento)
            return
        except TimeoutException:
            continue
    raise TimeoutException("Botão/link não encontrado (clique errado ou layout do site diferente)")

def entrar_no_iframe_conteudo(driver):
    try:
        iframe = WebDriverWait(driver, TEMPO_ESPERA).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//iframe[contains(@src,'administrativo') or contains(@src,'entrada')]"
            ))
        )
    except Exception:
        iframe = WebDriverWait(driver, TEMPO_ESPERA).until(
            EC.presence_of_element_located((By.XPATH, "//iframe"))
        )
    driver.switch_to.frame(iframe)

def entrar_no_iframe_que_contem(driver, xpath_do_elemento, msg_erro=None):
    driver.switch_to.default_content()
    iframes = driver.find_elements(By.XPATH, "//iframe")
    for idx, _ in enumerate(iframes):
        try:
            driver.switch_to.default_content()
            frame = driver.find_elements(By.XPATH, "//iframe")[idx]
            driver.switch_to.frame(frame)
            driver.find_element(By.XPATH, xpath_do_elemento)
            return
        except Exception:
            continue
    driver.switch_to.default_content()
    raise Exception(msg_erro or "Menu não encontrado (layout do site diferente)")

def normalizar_texto(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s).strip().lower())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")

def selecionar_option_por_texto(driver, select_id, texto):
    script = f"""
        const select = document.getElementById("{select_id}");
        const options = select.options;
        for (let i = 0; i < options.length; i++) {{
            if (options[i].text.trim().toLowerCase() === "{texto.lower()}") {{
                options[i].selected = true;
                select.dispatchEvent(new Event('change', {{ bubbles: true }}));
                return true;
            }}
        }}
        return false;
    """
    return driver.execute_script(script)

def selecionar_situacao_nao_pagos_por_clique(driver, logger):
    try:
        select = WebDriverWait(driver, TEMPO_ESPERA).until(
            EC.presence_of_element_located((By.ID, "situacao"))
        )
    except TimeoutException:
        raise TimeoutException("Dropdown 'Situação' não encontrado")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", select)
    time.sleep(0.5)
    select.click()
    time.sleep(1)
    alvo_norm = normalizar_texto("Não Pagos")
    options = select.find_elements(By.TAG_NAME, "option")
    for opt in options:
        opt_text = (opt.text or "").strip()
        opt_norm = normalizar_texto(opt_text)
        if not opt_norm:
            continue
        if opt_norm == alvo_norm or "nao pagos" in opt_norm:
            try:
                ActionChains(driver).move_to_element(opt).click().perform()
            except Exception:
                try:
                    opt.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", opt)
            time.sleep(1.5)
            logger.debug("Situação alterada para Não Pagos")
            return True
    logger.warning("Opção 'Não Pagos' não encontrada")
    return False

def selecionar_varios_eventos_flexivel(driver, select_id, lista_nomes_eventos, logger=None):
    if logger is None:
        logger = logging.getLogger("BOT")
    if not lista_nomes_eventos:
        return True
    select = WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.presence_of_element_located((By.ID, select_id))
    )
    options = select.find_elements(By.TAG_NAME, "option")
    indices_selecionar = []
    eventos_nao_encontrados = []
    lista_validos = [e for e in lista_nomes_eventos if normalizar_texto(e)]
    for evento_buscado in lista_nomes_eventos:
        texto_norm = normalizar_texto(evento_buscado)
        if not texto_norm:
            continue
        melhor_indice = None
        for i, opt in enumerate(options):
            opt_text = (opt.text or "").strip()
            opt_norm = normalizar_texto(opt_text)
            if not opt_norm:
                continue
            if opt_norm == texto_norm:
                melhor_indice = i
                break
            if texto_norm in opt_norm and melhor_indice is None:
                melhor_indice = i
        if melhor_indice is not None:
            indices_selecionar.append(melhor_indice)
        else:
            eventos_nao_encontrados.append(evento_buscado)
    if not indices_selecionar:
        logger.warning(f"Eventos não encontrados: {lista_nomes_eventos}")
        return False
    driver.execute_script(
        """
        var select = arguments[0];
        var indices = arguments[1];
        for (var j = 0; j < select.options.length; j++) { select.options[j].selected = false; }
        for (var k = 0; k < indices.length; k++) { select.options[indices[k]].selected = true; }
        select.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        select, indices_selecionar,
    )
    logger.debug(f"Selecionados {len(indices_selecionar)} evento(s)")
    # Após selecionar os eventos no filtro, clicar no botão "OK" do componente KSI para aplicar o filtro.
    try:
        clicar_xpath_js(driver, '//*[@id="ksiFiltro"]/ksi_filtro_conteudo/form/div/div[1]/div/div/div/p[1]')
        time.sleep(1.5)
        logger.debug("Botão OK do filtro de eventos clicado com sucesso.")
    except TimeoutException:
        logger.warning("Botão OK do filtro de eventos não encontrado após seleção; verifique o layout da tela.")
    except Exception as e:
        logger.warning(f"Falha ao clicar no botão OK do filtro de eventos: {e}")
    if len(indices_selecionar) != len(lista_validos):
        logger.warning(f"Eventos diferentes no site: {eventos_nao_encontrados}")
        return False
    return True

def preencher_data(driver, campo_id, data):
    elemento = WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.presence_of_element_located((By.ID, campo_id))
    )
    driver.execute_script(
        "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
        elemento, data
    )

def datas_mes_atual():
    hoje = datetime.now()
    ano, mes = hoje.year, hoje.month
    primeiro_dia = f"01/{mes:02d}/{ano}"
    ultimo_dia_num = calendar.monthrange(ano, mes)[1]
    ultimo_dia = f"{ultimo_dia_num:02d}/{mes:02d}/{ano}"
    return primeiro_dia, ultimo_dia

def competencia_de_data_inicio(data_inicio):
    try:
        partes = data_inicio.strip().split("/")
        if len(partes) == 3:
            return f"{partes[2]}-{partes[1]}"
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m")

def tipo_planilha_do_arquivo(caminho_arquivo):
    nome = os.path.basename(caminho_arquivo or "").upper()
    if "_NAO_PAGOS_" in nome:
        return "nao_pagos"
    if "_PAGOS_" in nome:
        return "pagos"
    if "NOVAS_LOCACOES" in nome or "NOVAS LOCACOES" in nome.replace("_", " "):
        return "novas_locacoes"
    return "pagos"

def tipo_para_api(tipo_interno):
    if tipo_interno == "novas_locacoes":
        return "novos_cadastros"
    return tipo_interno

def tipo_para_log(tipo_planilha):
    if tipo_planilha == "novos_cadastros":
        return "novas_locacoes"
    return tipo_planilha or "geral"

def slug_para_log(s):
    import re
    s = normalizar_texto(s or "")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s[:80] if s else "imobiliaria"

def registrar_erro_imobiliaria(nome_imob, tipo_planilha, mensagem, competencia=None, etapa="robo", exc=None, imobiliaria_id=None):
    try:
        os.makedirs(ERROS_IMOB_DIR, exist_ok=True)
        nome_slug = slug_para_log(nome_imob)
        dir_imob = os.path.join(ERROS_IMOB_DIR, nome_slug)
        dir_data = os.path.join(dir_imob, datetime.now().strftime("%d-%m"))
        os.makedirs(dir_data, exist_ok=True)
        tipo_log = tipo_para_log(tipo_planilha)
        caminho_log = os.path.join(dir_data, f"{tipo_log}.log")
        linhas = [
            "---",
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Etapa: {etapa}",
            f"Imobiliaria: {nome_imob or 'N/A'}",
            f"Imobiliaria ID: {imobiliaria_id if imobiliaria_id is not None else 'N/A'}",
            f"Tipo: {tipo_log}",
            f"Competencia: {competencia or 'N/A'}",
            f"Mensagem: {mensagem}",
        ]
        if exc is not None:
            linhas.append(f"Detalhe: {exc}")
            trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).strip()
            if trace:
                linhas.append("Traceback:")
                linhas.append(trace)
        with open(caminho_log, "a", encoding="utf-8") as f:
            f.write("\n".join(linhas) + "\n")
    except Exception:
        pass

def enviar_arquivo_para_servidor(caminho_arquivo, imobiliaria_id, competencia, logger, tipo_planilha="pagos", nome_imob=None):
    if not caminho_arquivo or not os.path.isfile(caminho_arquivo):
        logger.warning("Arquivo não encontrado para envio.")
        registrar_erro_imobiliaria(
            nome_imob,
            tipo_planilha,
            "Arquivo não encontrado para envio ao servidor.",
            competencia=competencia,
            etapa="envio_api",
            imobiliaria_id=imobiliaria_id,
        )
        return False, {}
    tipo_enviar = tipo_planilha_do_arquivo(caminho_arquivo)
    tipo_api = tipo_para_api(tipo_enviar)
    try:
        with open(caminho_arquivo, "rb") as f:
            r = requests.post(
                API_UPLOAD_URL,
                headers={"X-API-KEY": API_UPLOAD_KEY},
                data={
                    "imobiliaria_id": imobiliaria_id,
                    "tipo_planilha": tipo_api,
                    "competencia": competencia,
                },
                files={"locatarios": (os.path.basename(caminho_arquivo), f)},
                timeout=120,
            )
        if r.status_code == 200:
            try:
                resp = r.json()
                info = {"inserted": resp.get("inserted", 0), "updated": resp.get("updated", 0), "skipped": resp.get("skipped", 0)}
            except Exception:
                info = {}
            logger.info(f"Enviado: {os.path.basename(caminho_arquivo)}")
            return True, info
        corpo_resposta = (r.text or "").strip()
        logger.warning(f"Envio falhou (erro {r.status_code})")
        registrar_erro_imobiliaria(
            nome_imob,
            tipo_planilha,
            f"Envio falhou com status HTTP {r.status_code}. Resposta: {corpo_resposta[:1000] or 'sem corpo'}",
            competencia=competencia,
            etapa="envio_api",
            imobiliaria_id=imobiliaria_id,
        )
        return False, {}
    except Exception as exc:
        logger.warning("Erro de conexão ao enviar")
        registrar_erro_imobiliaria(
            nome_imob,
            tipo_planilha,
            "Erro de conexão ao enviar arquivo para a API.",
            competencia=competencia,
            etapa="envio_api",
            exc=exc,
            imobiliaria_id=imobiliaria_id,
        )
        return False, {}

def slug_nome(s):
    s = str(s).strip()
    s = re.sub(r'[/\\:*?"<>|]', "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:80] if s else "arquivo"


def slug_ascii_nome(s):
    s = normalizar_texto(s or "")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s.upper()[:80] if s else "ARQUIVO"


def normalizar_chave_split(s):
    s = normalizar_texto(s or "")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)


def detectar_delimitador_csv(caminho):
    for encoding in ("utf-8", "cp1252"):
        try:
            with open(caminho, "r", encoding=encoding, errors="strict") as f:
                amostra = f.read(4096)
            break
        except UnicodeDecodeError:
            continue
    else:
        with open(caminho, "r", encoding="cp1252", errors="replace") as f:
            amostra = f.read(4096)
    if amostra.count(";") >= amostra.count(","):
        return ";"
    return ","


def ler_csv_com_encoding(caminho):
    delimitador = detectar_delimitador_csv(caminho)
    for encoding in ("utf-8", "cp1252"):
        try:
            with open(caminho, "r", encoding=encoding, errors="strict", newline="") as f:
                reader = csv.DictReader(f, delimiter=delimitador)
                return list(reader), reader.fieldnames or [], encoding, delimitador
        except UnicodeDecodeError:
            continue
    with open(caminho, "r", encoding="cp1252", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimitador)
        return list(reader), reader.fieldnames or [], "cp1252", delimitador


def nome_divisao_arquivo(nome_imob_base, valor_coluna, aliases=None):
    aliases = aliases or {}
    valor_norm = normalizar_chave_split(valor_coluna)
    return aliases.get(valor_norm)


def nome_imobiliaria_geral(nome_imob):
    return f"{nome_imob}-GERAL"


def copiar_arquivo_bruto_para_geral(caminho_arquivo, nome_imob, tipo_planilha, logger):
    if not caminho_arquivo or not os.path.isfile(caminho_arquivo):
        return None
    nome_geral = nome_imobiliaria_geral(nome_imob)
    destino_dir = pasta_download_imobiliaria(nome_geral, tipo_planilha)
    slug_original = slug_nome(nome_imob)
    slug_geral = slug_nome(nome_geral)
    base_nome, ext = os.path.splitext(os.path.basename(caminho_arquivo))
    if base_nome.startswith(f"eventos_totalizador_{slug_original}_"):
        base_nome = base_nome.replace(f"eventos_totalizador_{slug_original}_", f"eventos_totalizador_{slug_geral}_", 1)
    elif base_nome.startswith(f"novas_locacoes_{slug_original}_"):
        base_nome = base_nome.replace(f"novas_locacoes_{slug_original}_", f"novas_locacoes_{slug_geral}_", 1)
    caminho_destino = os.path.join(destino_dir, f"{base_nome}{ext}")
    try:
        shutil.copy2(caminho_arquivo, caminho_destino)
        logger.info(f"[{nome_imob}] Arquivo bruto preservado em {nome_geral}")
        return caminho_destino
    except OSError as exc:
        logger.warning(f"[{nome_imob}] Não foi possível preservar arquivo bruto em {nome_geral}: {exc}")
        return None


def dividir_arquivo_por_coluna(caminho_arquivo, nome_imob, tipo_planilha, logger, config_split):
    ext = os.path.splitext(caminho_arquivo)[1].lower()
    if ext != ".csv":
        logger.info(f"[{nome_imob}] Split ignorado para {os.path.basename(caminho_arquivo)}: formato {ext} ainda não suportado.")
        return [{"caminho": caminho_arquivo, "nome_imob": nome_imob}]

    coluna = normalizar_chave_split((config_split or {}).get("coluna"))
    aliases_raw = (config_split or {}).get("aliases") or {}
    aliases = {normalizar_chave_split(chave): valor for chave, valor in aliases_raw.items()}
    rows, fieldnames, encoding, delimitador = ler_csv_com_encoding(caminho_arquivo)
    if not rows or not fieldnames:
        return [{"caminho": caminho_arquivo, "nome_imob": nome_imob}]

    coluna_real = None
    for field in fieldnames:
        if normalizar_chave_split(field) == coluna:
            coluna_real = field
            break
    if not coluna_real:
        logger.warning(f"[{nome_imob}] Coluna '{coluna}' não encontrada; mantendo arquivo original.")
        return [{"caminho": caminho_arquivo, "nome_imob": nome_imob}]

    copiar_arquivo_bruto_para_geral(caminho_arquivo, nome_imob, tipo_planilha, logger)

    grupos = {}
    rows_matriz = []
    for row in rows:
        valor = (row.get(coluna_real) or "").strip()
        nome_destino = nome_divisao_arquivo(nome_imob, valor, aliases)
        if nome_destino:
            grupos.setdefault(nome_destino, []).append(row)
        else:
            rows_matriz.append(row)

    if not grupos:
        return [{"caminho": caminho_arquivo, "nome_imob": nome_imob}]

    base_nome, ext = os.path.splitext(os.path.basename(caminho_arquivo))
    slug_original = slug_nome(nome_imob)
    gerados = []
    for nome_destino, rows_grupo in grupos.items():
        destino_dir = pasta_download_imobiliaria(nome_destino, tipo_planilha)
        slug_destino = slug_nome(nome_destino)
        base_destino = base_nome.replace(slug_original, slug_destino, 1)
        caminho_destino = os.path.join(destino_dir, f"{base_destino}{ext}")
        with open(caminho_destino, "w", encoding=encoding, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimitador)
            writer.writeheader()
            writer.writerows(rows_grupo)
        gerados.append({"caminho": caminho_destino, "nome_imob": nome_destino})
        logger.info(f"[{nome_imob}] Arquivo separado: {nome_destino} ({len(rows_grupo)} linha(s))")

    if rows_matriz:
        with open(caminho_arquivo, "w", encoding=encoding, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimitador)
            writer.writeheader()
            writer.writerows(rows_matriz)
        gerados.append({"caminho": caminho_arquivo, "nome_imob": nome_imob})
        logger.info(f"[{nome_imob}] Registros sem unidade mapeada mantidos na matriz ({len(rows_matriz)} linha(s))")
    else:
        try:
            os.remove(caminho_arquivo)
        except OSError as exc:
            logger.warning(f"[{nome_imob}] Não foi possível remover o arquivo consolidado após split: {exc}")

    return gerados


def pos_processar_arquivo_baixado(caminho_arquivo, imob, tipo_planilha, logger):
    if not caminho_arquivo or not os.path.isfile(caminho_arquivo):
        return []
    config_split = (imob or {}).get("split_download")
    nome_imob = (imob or {}).get("nome") or "IMOB"
    if not config_split:
        return [{"caminho": caminho_arquivo, "nome_imob": nome_imob}]
    return dividir_arquivo_por_coluna(caminho_arquivo, nome_imob, tipo_planilha, logger, config_split)


def obter_config_split(imob):
    return (imob or {}).get("split_download") or {}


def obter_aliases_split(imob):
    aliases_raw = obter_config_split(imob).get("aliases") or {}
    return {normalizar_chave_split(chave): valor for chave, valor in aliases_raw.items()}


def obter_opcoes_select_por_xpath(driver, xpath):
    select = WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )
    return driver.execute_script(
        """
        const select = arguments[0];
        return Array.from(select.options).map(opt => ({
            value: opt.value || '',
            text: (opt.textContent || '').trim()
        }));
        """,
        select,
    )


def selecionar_opcoes_select_por_xpath(driver, xpath, textos_desejados):
    textos_norm = [normalizar_chave_split(t) for t in (textos_desejados or []) if normalizar_chave_split(t)]
    select = WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )
    return driver.execute_script(
        """
        const select = arguments[0];
        const desejados = arguments[1];
        let selecionados = 0;
        for (const opt of Array.from(select.options)) {
            const norm = (opt.textContent || '')
                .normalize('NFD')
                .replace(/[\u0300-\u036f]/g, '')
                .trim()
                .toLowerCase();
            const match = desejados.includes(norm);
            opt.selected = match;
            if (match) {
                selecionados += 1;
            }
        }
        select.dispatchEvent(new Event('change', { bubbles: true }));
        return selecionados;
        """,
        select,
        textos_norm,
    )


def selecionar_nenhuma_unidade(driver, xpath):
    select = WebDriverWait(driver, TEMPO_ESPERA).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )
    driver.execute_script(
        """
        const select = arguments[0];
        for (const opt of Array.from(select.options)) {
            opt.selected = false;
        }
        select.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        select,
    )


def classificar_unidades_novas_locacoes(imob, opcoes):
    aliases = obter_aliases_split(imob)
    reconhecidas = []
    nao_reconhecidas = []
    for opcao in opcoes:
        texto = (opcao.get("text") or "").strip()
        if not texto or normalizar_chave_split(texto) in ("", "todos", "todas"):
            continue
        nome_destino = aliases.get(normalizar_chave_split(texto))
        if nome_destino:
            reconhecidas.append({"texto": texto, "nome_imob": nome_destino})
        else:
            nao_reconhecidas.append(texto)
    return reconhecidas, nao_reconhecidas


def pesquisar_novas_locacoes(driver):
    clicar_primeiro_xpath(driver, ["/html/body/ksi_card[2]/form/div/div[9]/div/label[2]", "//label[contains(., 'Dados')]"], tempo_por_tentativa=4)
    time.sleep(0.5)
    clicar_primeiro_xpath(driver, ["/html/body/ksi_card[2]/form/div/div[10]/button", "//button[contains(., 'Pesquisar')]"], tempo_por_tentativa=5)
    time.sleep(20)


def baixar_excel_novas_locacoes(driver, logger, nome_destino, data_inicio):
    clicar_primeiro_xpath(driver, ["//*[@id='DataTables_Table_0_wrapper']/div[1]/button", "//button[contains(., 'Excel')]"], tempo_por_tentativa=8)
    nome_final = f"novas_locacoes_{slug_nome(nome_destino)}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xls"
    return aguardar_e_renomear_download(
        logger,
        tempo_max=60,
        nome_final=nome_final,
        destino_dir=pasta_download_imobiliaria(nome_destino, "novas_locacoes", data_inicio),
    )


def baixar_novas_locacoes_com_unidades(driver, logger, imob, nome_imob, data_inicio):
    arquivos = []
    unidade_xpath = obter_config_split(imob).get("unidade_xpath") or '//*[@id="id_unidade"]'
    opcoes = obter_opcoes_select_por_xpath(driver, unidade_xpath)
    reconhecidas, nao_reconhecidas = classificar_unidades_novas_locacoes(imob, opcoes)

    selecionar_nenhuma_unidade(driver, unidade_xpath)
    pesquisar_novas_locacoes(driver)
    caminho_geral = baixar_excel_novas_locacoes(driver, logger, nome_imobiliaria_geral(nome_imob), data_inicio)
    if caminho_geral:
        logger.info(f"[{nome_imob}] Novas locações bruto preservado em {nome_imobiliaria_geral(nome_imob)}")

    for unidade in reconhecidas:
        selecionados = selecionar_opcoes_select_por_xpath(driver, unidade_xpath, [unidade["texto"]])
        if not selecionados:
            logger.warning(f"[{nome_imob}] Unidade não pôde ser selecionada em Novas locações: {unidade['texto']}")
            continue
        pesquisar_novas_locacoes(driver)
        caminho = baixar_excel_novas_locacoes(driver, logger, unidade["nome_imob"], data_inicio)
        if caminho:
            arquivos.append({"caminho": caminho, "nome_imob": unidade["nome_imob"]})
            logger.info(f"[{nome_imob}] Novas locações separado: {unidade['nome_imob']}")

    if nao_reconhecidas:
        selecionados = selecionar_opcoes_select_por_xpath(driver, unidade_xpath, nao_reconhecidas)
        if selecionados:
            pesquisar_novas_locacoes(driver)
            caminho = baixar_excel_novas_locacoes(driver, logger, nome_imob, data_inicio)
            if caminho:
                arquivos.append({"caminho": caminho, "nome_imob": nome_imob})
                logger.info(f"[{nome_imob}] Novas locações sem unidade mapeada mantido na matriz ({selecionados} unidade(s))")

    if not reconhecidas and not nao_reconhecidas:
        caminho = baixar_excel_novas_locacoes(driver, logger, nome_imob, data_inicio)
        if caminho:
            copiar_arquivo_bruto_para_geral(caminho, nome_imob, "novas_locacoes", logger)
            arquivos.append({"caminho": caminho, "nome_imob": nome_imob})

    return arquivos

def download_novas_locacoes(driver, logger, imob=None, imobiliaria_id=None, competencia=None, data_inicio=None, data_fim=None):
    nome_imob = (imob or {}).get("nome") or "IMOB"
    try:
        logger.debug("Abrindo menu Relatórios...")
        driver.switch_to.default_content()
        time.sleep(1)
        clicar_primeiro_xpath(driver, [
            "//a[normalize-space()='Relatórios']", "//a[contains(., 'Relatórios')]",
            "//a[contains(., 'Relatorios')]", "/html/body/div[5]/div/div[1]/div[2]/ul/li[7]/a",
        ], tempo_por_tentativa=5)
        time.sleep(3)
        try:
            entrar_no_iframe_que_contem(driver, "//a[contains(., 'Estatistica') or contains(., 'Planilhas')]", "Relatórios não encontrado")
        except Exception:
            entrar_no_iframe_conteudo(driver)
        time.sleep(2)
        clicar_primeiro_xpath(driver, ["//*[@id='non-printable']/ksi_card_botoes/a[1]", "//a[contains(., 'Estatistica')]"], tempo_por_tentativa=5)
        time.sleep(2)
        clicar_primeiro_xpath(driver, ["/html/body/ksi_botoes_destaque/a[5]", "//a[contains(., 'Planilhas') and contains(., 'nova')]"], tempo_por_tentativa=5)
        time.sleep(2)
        clicar_primeiro_xpath(driver, ["/html/body/ksi_botoes_destaque[2]/a[1]", "//a[contains(., 'Novas locações')]"], tempo_por_tentativa=5)
        time.sleep(2)
        if not data_inicio or not data_fim:
            hoje = datetime.now()
            data_inicio = (hoje - timedelta(days=45)).strftime("%d/%m/%Y")
            data_fim = hoje.strftime("%d/%m/%Y")
        preencher_data(driver, "data_ini", data_inicio)
        time.sleep(0.5)
        preencher_data(driver, "data_fim", data_fim)
        time.sleep(0.5)
        config_split = obter_config_split(imob)
        if config_split.get("aliases"):
            return baixar_novas_locacoes_com_unidades(driver, logger, imob, nome_imob, data_inicio)
        pesquisar_novas_locacoes(driver)
        caminho = baixar_excel_novas_locacoes(driver, logger, nome_imob, data_inicio)
        if caminho:
            return [{"caminho": caminho, "nome_imob": nome_imob}]
        return []
    except Exception as exc:
        logger.warning("Erro no download de Novas locações")
        registrar_erro_imobiliaria(
            nome_imob,
            "novas_locacoes",
            "Erro no download de Novas locações.",
            competencia=competencia,
            etapa="download_novas_locacoes",
            exc=exc,
            imobiliaria_id=imobiliaria_id,
        )
        return []


def obter_senha_para_imobiliaria(imob):
    """
    Retorna a senha a ser utilizada para login, aplicando regras especiais
    para algumas imobiliárias (como ESTRUTURA) e caindo no padrão quando
    não houver senha específica.
    """
    imob = imob or {}
    senha_template = (imob.get("senha_template") or "").strip()
    if senha_template:
        dia = datetime.now().day
        return senha_template.format(dia=dia, dia2=f"{dia:02d}")

    senha_fixa = (imob.get("senha") or "").strip()
    if senha_fixa:
        return senha_fixa

    if LOGIN_SENHA:
        return LOGIN_SENHA

    if LOGIN_SENHA_TEMPLATE:
        dia = datetime.now().day
        return LOGIN_SENHA_TEMPLATE.format(dia=dia, dia2=f"{dia:02d}")

    return ""

def fazer_login_e_ir_para_adm_locacao(driver, logger, imob=None):
    usuario = (imob or {}).get("login") or LOGIN_EMAIL
    senha = obter_senha_para_imobiliaria(imob)
    digitar_xpath(driver, '//*[@id="usuario"]', usuario)
    digitar_xpath(driver, '//*[@id="senha"]', senha)
    clicar_xpath(driver, "/html/body/div[1]/div[2]/div/div/div[1]/div/div/form/div[4]/div/div[2]/button")
    time.sleep(5)
    clicar_xpath(driver, "/html/body/table/tbody/tr[13]/td/table/tbody/tr/td/a")
    time.sleep(2)
    clicar_primeiro_xpath(driver, [
        "//a[normalize-space()='Adm. Locação']", "//a[contains(., 'Adm. Locação')]",
        "//a[contains(., 'Adm. Locacao')]", "/html/body/div[5]/div/div[1]/div[2]/ul/li[2]/a",
    ], tempo_por_tentativa=5)

def navegar_ate_totalizador(driver, logger):
    time.sleep(2)
    xpath_eventos = "//a[contains(normalize-space(), 'Eventos') and contains(normalize-space(), 'Lançamentos')]"
    entrar_no_iframe_que_contem(driver, xpath_eventos, "Menu Eventos Lançamentos não encontrado")
    clicar_xpath_js(driver, xpath_eventos)
    clicar_xpath(driver, "/html/body/ksi_botoes_destaque/a[3]")

def _safe_getmtime(path):
    try:
        return os.path.getmtime(path)
    except (OSError, FileNotFoundError):
        return 0

def aguardar_e_renomear_download(logger, tempo_max=60, nome_final=None, destino_dir=None):
    import glob
    if nome_final is None:
        nome_final = f"eventos_totalizador_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xls"
    if destino_dir is None:
        destino_dir = DOWNLOAD_DIR
    os.makedirs(destino_dir, exist_ok=True)
    base_final = os.path.splitext(nome_final)[0]
    tempo_min = int(time.time()) - 180
    for _ in range(tempo_max):
        if glob.glob(os.path.join(DOWNLOAD_DIR, "*.crdownload")):
            time.sleep(1)
            continue
        break
    else:
        logger.warning("Download falhou - arquivo não apareceu a tempo.")
        return None
    time.sleep(2)
    base_lower = base_final.lower()
    for tentativa in range(tempo_max):
        todos_candidatos = []
        for ext in ("*.tmp", "*.xls", "*.xlsx", "*.csv"):
            todos_candidatos.extend(glob.glob(os.path.join(DOWNLOAD_DIR, ext)))
        todos_candidatos = [c for c in todos_candidatos if os.path.exists(c) and _safe_getmtime(c) >= tempo_min]
        candidatos = [c for c in todos_candidatos if not os.path.basename(c).startswith(base_final + ".")]
        def _nome_arquivo(c):
            return os.path.basename(c).lower()
        if "_pagos_" in base_lower and "_nao_pagos_" not in base_lower:
            candidatos = [c for c in candidatos if "_nao_pagos_" not in _nome_arquivo(c) and "novas_locacoes" not in _nome_arquivo(c)]
        elif "_nao_pagos_" in base_lower:
            candidatos = [c for c in candidatos if not ("_pagos_" in _nome_arquivo(c) and "_nao_pagos_" not in _nome_arquivo(c)) and "novas_locacoes" not in _nome_arquivo(c)]
        elif "novas_locacoes" in base_lower:
            candidatos = [c for c in candidatos if "_pagos_" not in _nome_arquivo(c) and "_nao_pagos_" not in _nome_arquivo(c)]
        if not candidatos and tentativa >= tempo_max // 2:
            # Fallback: depois de metade do tempo, relaxa os filtros por tipo
            candidatos = todos_candidatos
        if not candidatos:
            time.sleep(1)
            continue
        mais_recente = max(candidatos, key=_safe_getmtime)
        if _safe_getmtime(mais_recente) == 0:
            time.sleep(1)
            continue
        ext_baixado = os.path.splitext(mais_recente)[1].lower() or ".xls"
        nome_final_com_ext = base_final + ext_baixado
        caminho_final = os.path.join(destino_dir, nome_final_com_ext)
        if os.path.abspath(mais_recente) == os.path.abspath(caminho_final):
            return caminho_final
        try:
            tam_antes = os.path.getsize(mais_recente)
        except (OSError, FileNotFoundError):
            time.sleep(1)
            continue
        time.sleep(2)
        if not os.path.exists(mais_recente):
            time.sleep(1)
            continue
        try:
            tam_depois = os.path.getsize(mais_recente)
        except (OSError, FileNotFoundError):
            time.sleep(1)
            continue
        if tam_antes != tam_depois:
            time.sleep(1)
            continue
        time.sleep(1)
        try:
            if not os.path.exists(mais_recente):
                time.sleep(1)
                continue
            shutil.move(mais_recente, caminho_final)
            return caminho_final
        except OSError:
            if os.path.exists(caminho_final):
                return caminho_final
            time.sleep(1)
    for ext in (".csv", ".xls", ".xlsx"):
        caminho_teste = os.path.join(destino_dir, base_final + ext)
        if os.path.exists(caminho_teste) and _safe_getmtime(caminho_teste) >= tempo_min:
            return caminho_teste
    logger.warning(f"Arquivo não encontrado: {base_final}")
    return None

def filtrar_por_nome(imobiliarias, nomes_busca):
    if not nomes_busca:
        return imobiliarias
    termos = [normalizar_texto(n) for n in nomes_busca if n]
    if not termos:
        return imobiliarias
    resultado = []
    for imob in imobiliarias:
        nome = imob.get("nome", "")
        nome_norm = normalizar_texto(nome)
        for t in termos:
            if t in nome_norm or nome_norm in t:
                resultado.append(imob)
                break
    return resultado


# =========================
# FLUXO PRINCIPAL
# =========================
def executar(tipo_planilha, imobiliarias=None, data_inicio=None, data_fim=None):
    """
    tipo_planilha: 'pagos' | 'nao_pagos' | 'novas_locacoes'
    data_inicio, data_fim: opcionais, formato dd/mm/yyyy (ex: 01/02/2026, 28/02/2026)
    """
    if imobiliarias is None:
        imobiliarias = IMOBILIARIAS_EVENTOS

    if tipo_planilha == "novas_locacoes":
        imobiliarias = [i for i in imobiliarias if i.get("novas_locacoes", False)]
        if not imobiliarias:
            print("Nenhuma imobiliária com novas_locacoes habilitado.")
            return

    os.makedirs(LOGS_DIR, exist_ok=True)
    logger = configurar_logger(tipo_planilha)
    logger.info(f"🚀 Bot iniciado — {tipo_planilha or 'TODOS'}")
    if len(imobiliarias) < len(IMOBILIARIAS_EVENTOS):
        logger.info(f"Filtrado: {len(imobiliarias)} imobiliária(s)")

    driver = None
    planilhas_para_envio = []
    resultados_por_imob = []
    if data_inicio is None or data_fim is None:
        data_inicio, data_fim = datas_mes_atual()
    try:
        driver = iniciar_driver()
        logger.info(f"Período: {data_inicio} até {data_fim}")

        for idx_imob, imob in enumerate(imobiliarias):
            nome_imob = imob.get("nome", "IMOB")
            eventos = imob.get("eventos", [])
            url_imob = imob.get("url")

            if not url_imob:
                logger.warning(f"{nome_imob} sem URL; pulando.")
                registrar_erro_imobiliaria(
                    nome_imob,
                    tipo_planilha,
                    "Imobiliária sem URL configurada.",
                    competencia=competencia_de_data_inicio(data_inicio),
                    etapa="configuracao",
                    imobiliaria_id=imob.get("id_imobiliaria"),
                )
                resultados_por_imob.append({"imob": nome_imob, "status": "pulado", "erro": "Sem URL configurada"})
                continue

            concluido = False
            ultimo_erro = None
            etapa_atual = "inicializacao"
            for tentativa in range(1, MAX_TENTATIVAS_IMOBILIARIA + 1):
                try:
                    logger.debug(f"[{idx_imob + 1}/{len(imobiliarias)}] {nome_imob} — tentativa {tentativa}")
                    etapa_atual = "abrindo_url"
                    driver.get(url_imob)
                    time.sleep(3)
                    etapa_atual = "login"
                    fazer_login_e_ir_para_adm_locacao(driver, logger, imob)

                    # Novas locações usa menu Relatórios (não precisa do Totalizador)
                    if tipo_planilha == "novas_locacoes":
                        competencia = datetime.now().strftime("%Y-%m")
                        etapa_atual = "download_novas_locacoes"
                        arquivos_novas = download_novas_locacoes(
                            driver,
                            logger,
                            imob,
                            imob.get("id_imobiliaria"),
                            competencia,
                            data_inicio,
                            data_fim,
                        )
                        if arquivos_novas:
                            logger.info(f"[{nome_imob}] Novas locações concluído")
                            id_imob = imob.get("id_imobiliaria")
                            if id_imob is not None:
                                for arquivo in arquivos_novas:
                                    planilhas_para_envio.append({
                                        "caminho": arquivo["caminho"], "id_imobiliaria": id_imob,
                                        "competencia": competencia, "tipo_planilha": "novas_locacoes", "nome_imob": arquivo["nome_imob"],
                                    })
                            resultados_por_imob.append({"imob": nome_imob, "status": "sucesso", "erro": None, "msg": ["Novas locações concluído"]})
                        else:
                            registrar_erro_imobiliaria(
                                nome_imob,
                                "novas_locacoes",
                                "Download não concluído.",
                                competencia=competencia,
                                etapa="download_novas_locacoes",
                                imobiliaria_id=imob.get("id_imobiliaria"),
                            )
                            resultados_por_imob.append({"imob": nome_imob, "status": "falha", "erro": "Download não concluído", "msg": []})
                        break

                    etapa_atual = "navegacao_totalizador"
                    navegar_ate_totalizador(driver, logger)
                    MAX_TENTATIVAS_EVENTOS = 2
                    eventos_ok = False
                    for tentativa_ev in range(MAX_TENTATIVAS_EVENTOS):
                        logger.info(f"[{nome_imob}] Eventos: {', '.join(eventos)}")
                        etapa_atual = "selecionando_eventos"
                        eventos_ok = selecionar_varios_eventos_flexivel(driver, "eventos_tipo", eventos, logger)
                        if eventos_ok:
                            break
                        if tentativa_ev < MAX_TENTATIVAS_EVENTOS - 1:
                            etapa_atual = "reabrindo_totalizador_eventos"
                            navegar_ate_totalizador(driver, logger)
                            time.sleep(2)
                    if not eventos_ok:
                        raise Exception("Eventos não encontrados no dropdown")

                    etapa_atual = "preenchendo_periodo"
                    preencher_data(driver, "data_inicio", data_inicio)
                    preencher_data(driver, "data_fim", data_fim)
                    etapa_atual = "selecionando_conta"
                    selecionar_option_por_texto(driver, "conta", "Imobiliária")

                    # --- PAGOS ---
                    if tipo_planilha in (None, "pagos"):
                        etapa_atual = "selecionando_situacao_pagos"
                        selecionar_option_por_texto(driver, "situacao", "Pagos")
                        logger.debug("Clicando Pesquisar (Pagos)")
                        etapa_atual = "pesquisando_pagos"
                        clicar_primeiro_xpath(driver, [
                            '//*[@id="ksiFiltro"]//button[contains(., "Pesquisar")]',
                            '//button[contains(., "Pesquisar")]',
                        ], tempo_por_tentativa=4)
                        time.sleep(30)
                        logger.debug("Baixar Excel (Pagos)")
                        etapa_atual = "baixando_pagos"
                        clicar_primeiro_xpath(driver, [
                            '//a[@title="Exportar para Excel."]',
                            '//a[contains(@href, "excel=1")]',
                            '//ksi_botoes_destaque/a[1]',
                        ], tempo_por_tentativa=5)
                        nome_arquivo_pagos = f"eventos_totalizador_{slug_nome(nome_imob)}_pagos_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xls"
                        caminho_pagos = aguardar_e_renomear_download(
                            logger,
                            tempo_max=60,
                            nome_final=nome_arquivo_pagos,
                            destino_dir=pasta_download_imobiliaria(nome_imob, "pagos", data_inicio),
                        )
                        if caminho_pagos:
                            logger.info(f"[{nome_imob}] PAGOS concluído")
                            id_imob = imob.get("id_imobiliaria")
                            competencia = competencia_de_data_inicio(data_inicio)
                            arquivos_processados = pos_processar_arquivo_baixado(caminho_pagos, imob, "pagos", logger)
                            if id_imob is not None:
                                for arquivo in arquivos_processados:
                                    planilhas_para_envio.append({
                                        "caminho": arquivo["caminho"], "id_imobiliaria": id_imob,
                                        "competencia": competencia, "tipo_planilha": "pagos", "nome_imob": arquivo["nome_imob"],
                                    })
                        if tipo_planilha == "pagos":
                            concluido = True
                            resultados_por_imob.append({"imob": nome_imob, "status": "sucesso", "erro": None, "msg": ["Pagos concluído"]})
                            logger.info(f"{nome_imob} - PAGOS concluído")
                            break

                    # --- NÃO PAGOS ---
                    if tipo_planilha in (None, "nao_pagos"):
                        if tipo_planilha == "nao_pagos":
                            etapa_atual = "resetando_filtro_nao_pagos"
                            selecionar_option_por_texto(driver, "situacao", "Pagos")
                            clicar_primeiro_xpath(driver, ['//button[contains(., "Pesquisar")]'], tempo_por_tentativa=4)
                            time.sleep(30)
                        etapa_atual = "selecionando_situacao_nao_pagos"
                        selecionar_situacao_nao_pagos_por_clique(driver, logger)
                        time.sleep(2)
                        etapa_atual = "pesquisando_nao_pagos"
                        clicar_primeiro_xpath(driver, [
                            '//*[@id="ksiFiltro"]//button[contains(., "Pesquisar")]',
                            '//button[contains(., "Pesquisar")]',
                        ], tempo_por_tentativa=4)
                        time.sleep(25)
                        xpaths_excel = ['//a[@title="Exportar para Excel."]', '//a[contains(@href, "excel=1")]', '//ksi_botoes_destaque/a[1]']
                        try:
                            etapa_atual = "baixando_nao_pagos"
                            for xp in xpaths_excel:
                                try:
                                    el = WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, xp)))
                                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                                    time.sleep(1)
                                    el.click()
                                    break
                                except TimeoutException:
                                    continue
                            else:
                                raise TimeoutException("Botão Excel não encontrado")
                        except Exception as e:
                            logger.warning(f"Alternativa Excel: {e}")
                            clicar_primeiro_xpath(driver, xpaths_excel, tempo_por_tentativa=8)
                        nome_arquivo_nao_pagos = f"eventos_totalizador_{slug_nome(nome_imob)}_nao_pagos_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xls"
                        caminho_nao_pagos = aguardar_e_renomear_download(
                            logger,
                            tempo_max=60,
                            nome_final=nome_arquivo_nao_pagos,
                            destino_dir=pasta_download_imobiliaria(nome_imob, "nao_pagos", data_inicio),
                        )
                        if caminho_nao_pagos:
                            logger.info(f"[{nome_imob}] Não pagos concluído")
                            id_imob = imob.get("id_imobiliaria")
                            competencia = competencia_de_data_inicio(data_inicio)
                            arquivos_processados = pos_processar_arquivo_baixado(caminho_nao_pagos, imob, "nao_pagos", logger)
                            if id_imob is not None:
                                for arquivo in arquivos_processados:
                                    planilhas_para_envio.append({
                                        "caminho": arquivo["caminho"], "id_imobiliaria": id_imob,
                                        "competencia": competencia, "tipo_planilha": "nao_pagos", "nome_imob": arquivo["nome_imob"],
                                    })
                        if tipo_planilha == "nao_pagos":
                            concluido = True
                            resultados_por_imob.append({"imob": nome_imob, "status": "sucesso", "erro": None, "msg": ["Não pagos concluído"]})
                            logger.info(f"{nome_imob} - Não pagos concluído")
                            break

                    # --- NOVAS LOCAÇÕES (só quando tipo_planilha is None - já tratamos "novas_locacoes" acima) ---
                    caminho_novas = None
                    if tipo_planilha is None and imob.get("novas_locacoes", False):
                        etapa_atual = "download_novas_locacoes"
                        arquivos_novas = download_novas_locacoes(
                            driver,
                            logger,
                            imob,
                            imob.get("id_imobiliaria"),
                            competencia_de_data_inicio(data_inicio),
                            data_inicio,
                            data_fim,
                        )
                        caminho_novas = bool(arquivos_novas)
                        if arquivos_novas:
                            logger.info(f"[{nome_imob}] Novas locações concluído")
                            id_imob = imob.get("id_imobiliaria")
                            competencia = competencia_de_data_inicio(data_inicio)
                            if id_imob is not None:
                                for arquivo in arquivos_novas:
                                    planilhas_para_envio.append({
                                        "caminho": arquivo["caminho"], "id_imobiliaria": id_imob,
                                        "competencia": competencia, "tipo_planilha": "novas_locacoes", "nome_imob": arquivo["nome_imob"],
                                    })

                    if tipo_planilha is None:
                        msgs = ["Pagos concluído", "Não pagos concluído"]
                        if imob.get("novas_locacoes", False) and caminho_novas:
                            msgs.append("Novas locações concluído")
                        concluido = True
                        resultados_por_imob.append({"imob": nome_imob, "status": "sucesso", "erro": None, "msg": msgs})
                        logger.info(f"{nome_imob} - Executado com sucesso")
                        break

                except Exception as e:
                    s = str(e)
                    ultimo_erro = s[:120] if len(s) > 120 else s
                    logger.warning(f"{nome_imob} — tentativa {tentativa} falhou na etapa '{etapa_atual}': {ultimo_erro}")
                    registrar_erro_imobiliaria(
                        nome_imob,
                        tipo_planilha,
                        f"Tentativa {tentativa}/{MAX_TENTATIVAS_IMOBILIARIA} falhou.",
                        competencia=competencia_de_data_inicio(data_inicio),
                        etapa=etapa_atual,
                        exc=e,
                        imobiliaria_id=imob.get("id_imobiliaria"),
                    )
                    if tentativa < MAX_TENTATIVAS_IMOBILIARIA:
                        if driver:
                            try:
                                driver.quit()
                            except Exception:
                                pass
                        driver = iniciar_driver()
                        logger.info(f"{nome_imob} — retomando com nova sessão do navegador")
                    else:
                        resultados_por_imob.append({"imob": nome_imob, "status": "falha", "erro": ultimo_erro})
                        logger.error(f"{nome_imob} - Erro após {MAX_TENTATIVAS_IMOBILIARIA} tentativas")
                        logger.exception(f"Erro em {nome_imob}:")
                        registrar_erro_imobiliaria(
                            nome_imob,
                            tipo_planilha,
                            f"Erro após {MAX_TENTATIVAS_IMOBILIARIA} tentativas.",
                            competencia=competencia_de_data_inicio(data_inicio),
                            etapa=etapa_atual,
                            exc=e,
                            imobiliaria_id=imob.get("id_imobiliaria"),
                        )

    except Exception as e:
        logger.exception("❌ Erro durante a execução")

    finally:
        if driver:
            driver.quit()
            logger.info("Driver encerrado")

        if planilhas_para_envio:
            logger.info(f"Planilhas geradas: {len(planilhas_para_envio)}")
            sucesso, falha = [], []
            for p in planilhas_para_envio:
                ok, info = enviar_arquivo_para_servidor(
                    p["caminho"],
                    p["id_imobiliaria"],
                    p["competencia"],
                    logger,
                    p.get("tipo_planilha", "pagos"),
                    p.get("nome_imob"),
                )
                if ok:
                    sucesso.append({"nome": p.get("nome_imob", "?"), "tipo": p.get("tipo_planilha", "?")})
                else:
                    falha.append({"nome": p.get("nome_imob", "?"), "tipo": p.get("tipo_planilha", "?")})
            envio_servidor = "falha" if falha and not sucesso else ("sucesso" if sucesso and not falha else "parcial")
        else:
            envio_servidor = None

        try:
            dir_logs = LOGS_DIR
            os.makedirs(dir_logs, exist_ok=True)
            resumo = {
                "ultima_execucao": datetime.now().isoformat(),
                "tipo_planilha": tipo_planilha or "todos",
                "periodo": {"data_inicio": data_inicio, "data_fim": data_fim} if (data_inicio and data_fim) else None,
                "resultados": resultados_por_imob,
                "envio_servidor": envio_servidor,
                "arquivo_log": ROBO_LOG_ATUAL,
            }
            # JSON principal (sempre sobrescrito)
            with open(ROBO_RESUMO_PATH, "w", encoding="utf-8") as f:
                json.dump(resumo, f, ensure_ascii=False, indent=2)

            # TXT com mesmo conteúdo (para o site aceitar no "Carregar logs")
            robo_resumo_txt = os.path.join(dir_logs, "robo_resumo.txt")
            with open(robo_resumo_txt, "w", encoding="utf-8") as f_txt:
                json.dump(resumo, f_txt, ensure_ascii=False, indent=2)

            # JSON individual por execução (para download e uso no "Carregar logs")
            base_json = None
            if ROBO_LOG_ATUAL:
                base_nome = os.path.splitext(os.path.basename(ROBO_LOG_ATUAL))[0]
                base_json = f"{base_nome}.json"
            else:
                base_json = datetime.now().strftime("robo_resumo_%Y-%m-%d_%H-%M-%S.json")
            caminho_json_execucao = os.path.join(dir_logs, base_json)
            with open(caminho_json_execucao, "w", encoding="utf-8") as f_exec:
                json.dump(resumo, f_exec, ensure_ascii=False, indent=2)
        except Exception as ex:
            logger.error(f"Erro ao gravar resumo do robô em JSON: {ex}")

        logger.info("🏁 Execução finalizada")
