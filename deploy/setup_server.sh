#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ask() {
  local prompt="$1"
  local default="${2:-}"
  local answer
  if [ -n "$default" ]; then
    read -r -p "$prompt [$default]: " answer
    printf '%s' "${answer:-$default}"
  else
    read -r -p "$prompt: " answer
    printf '%s' "$answer"
  fi
}

ask_secret() {
  local prompt="$1"
  local answer
  read -r -s -p "$prompt: " answer
  printf '\n' >&2
  printf '%s' "$answer"
}

ask_yes_no() {
  local prompt="$1"
  local default="${2:-y}"
  local answer
  read -r -p "$prompt [${default}]: " answer
  answer="${answer:-$default}"
  case "${answer,,}" in
    y|yes|s|sim) return 0 ;;
    *) return 1 ;;
  esac
}

replace_in_file() {
  local file="$1"
  local search="$2"
  local replace="$3"
  python3 - <<PY
from pathlib import Path
path = Path(r"""$file""")
text = path.read_text(encoding="utf-8")
text = text.replace(r"""$search""", r"""$replace""")
path.write_text(text, encoding="utf-8")
PY
}

echo "Instalador do Robo VGT para servidor Linux"
echo "Projeto detectado em: ${PROJECT_ROOT}"
echo

INSTALL_DIR="$(ask 'Pasta final do projeto no servidor' '/opt/robo-vgt')"
SERVICE_USER="$(ask 'Usuario do sistema que vai rodar o robo' 'robo')"
DB_HOST="$(ask 'Host do banco' '186.209.113.149')"
DB_USER="$(ask 'Usuario do banco' 'launs_apolice')"
DB_PASSWORD="$(ask_secret 'Senha do banco')"
DB_NAME="$(ask 'Nome do banco' 'launs_apolice')"

INSTALL_APT=0
INSTALL_SYSTEMD=0
INSTALL_MONTHLY=0

if ask_yes_no 'Instalar pacotes do sistema com apt (python3-venv, pip, chrome deps)?' 'y'; then
  INSTALL_APT=1
fi

if ask_yes_no 'Instalar service e timer no systemd?' 'y'; then
  INSTALL_SYSTEMD=1
fi

if ask_yes_no 'Instalar tambem o fechamento mensal completo no systemd?' 'y'; then
  INSTALL_MONTHLY=1
fi

echo
echo "Resumo"
echo "- Projeto: ${PROJECT_ROOT}"
echo "- Destino: ${INSTALL_DIR}"
echo "- Usuario: ${SERVICE_USER}"
echo "- Banco: ${DB_HOST} / ${DB_NAME}"
echo "- Instalar apt: ${INSTALL_APT}"
echo "- Instalar systemd: ${INSTALL_SYSTEMD}"
echo "- Instalar fechamento mensal: ${INSTALL_MONTHLY}"
echo

if ! ask_yes_no 'Confirmar instalacao?' 'y'; then
  echo "Cancelado."
  exit 1
fi

if [ "${INSTALL_APT}" -eq 1 ]; then
  sudo apt-get update
  sudo apt-get install -y python3 python3-venv python3-pip unzip curl rsync ca-certificates fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgbm1 libgcc1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 lsb-release xdg-utils
fi

sudo mkdir -p "${INSTALL_DIR}"
sudo rsync -a --delete \
  --exclude '.env.server' \
  --exclude 'segredos/' \
  "${PROJECT_ROOT}/" "${INSTALL_DIR}/"

if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
  sudo useradd -r -m -s /bin/bash "${SERVICE_USER}"
fi

sudo mkdir -p "${INSTALL_DIR}/logs" "${INSTALL_DIR}/data" "${INSTALL_DIR}/downloads" "${INSTALL_DIR}/downloads_processados"
sudo mkdir -p "${INSTALL_DIR}/segredos"
sudo python3 -m venv "${INSTALL_DIR}/.venv"
sudo "${INSTALL_DIR}/.venv/bin/pip" install --upgrade pip
sudo "${INSTALL_DIR}/.venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt"

ENV_FILE="${INSTALL_DIR}/.env.server"
sudo cp "${INSTALL_DIR}/deploy/.env.server.example" "${ENV_FILE}"
sudo sed -i "s|^ROBO_DB_HOST=.*|ROBO_DB_HOST=${DB_HOST}|" "${ENV_FILE}"
sudo sed -i "s|^ROBO_DB_USER=.*|ROBO_DB_USER=${DB_USER}|" "${ENV_FILE}"
sudo sed -i "s|^ROBO_DB_PASSWORD=.*|ROBO_DB_PASSWORD=${DB_PASSWORD}|" "${ENV_FILE}"
sudo sed -i "s|^ROBO_DB_DATABASE=.*|ROBO_DB_DATABASE=${DB_NAME}|" "${ENV_FILE}"

if [ ! -f "${INSTALL_DIR}/segredos/robo.local.json" ]; then
  sudo cp "${INSTALL_DIR}/deploy/examples/robo.local.example.json" "${INSTALL_DIR}/segredos/robo.local.json"
fi
if [ ! -f "${INSTALL_DIR}/segredos/imobiliarias.local.json" ]; then
  sudo cp "${INSTALL_DIR}/deploy/examples/imobiliarias.local.example.json" "${INSTALL_DIR}/segredos/imobiliarias.local.json"
fi

sudo chmod +x "${INSTALL_DIR}/deploy/systemd/rodar_pagos_novas_competencia_anterior.sh"
sudo chmod +x "${INSTALL_DIR}/deploy/systemd/rodar_fechamento_mensal.sh"
sudo chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}"

replace_in_file "${INSTALL_DIR}/deploy/systemd/rodar_pagos_novas_competencia_anterior.sh" \
  'PROJECT_ROOT="/opt/robo-vgt"' \
  "PROJECT_ROOT=\"${INSTALL_DIR}\""

replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.service" \
  'User=robo' \
  "User=${SERVICE_USER}"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.service" \
  'WorkingDirectory=/opt/robo-vgt' \
  "WorkingDirectory=${INSTALL_DIR}"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.service" \
  'ExecStart=/opt/robo-vgt/deploy/systemd/rodar_pagos_novas_competencia_anterior.sh' \
  "ExecStart=${INSTALL_DIR}/deploy/systemd/rodar_pagos_novas_competencia_anterior.sh"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.service" \
  'StandardOutput=append:/opt/robo-vgt/logs/systemd_pagos_novas.log' \
  "StandardOutput=append:${INSTALL_DIR}/logs/systemd_pagos_novas.log"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.service" \
  'StandardError=append:/opt/robo-vgt/logs/systemd_pagos_novas.log' \
  "StandardError=append:${INSTALL_DIR}/logs/systemd_pagos_novas.log"

replace_in_file "${INSTALL_DIR}/deploy/systemd/rodar_fechamento_mensal.sh" \
  'PROJECT_ROOT="/opt/robo-vgt"' \
  "PROJECT_ROOT=\"${INSTALL_DIR}\""

replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.service" \
  'User=robo' \
  "User=${SERVICE_USER}"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.service" \
  'WorkingDirectory=/opt/robo-vgt' \
  "WorkingDirectory=${INSTALL_DIR}"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.service" \
  'ExecStart=/opt/robo-vgt/deploy/systemd/rodar_fechamento_mensal.sh' \
  "ExecStart=${INSTALL_DIR}/deploy/systemd/rodar_fechamento_mensal.sh"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.service" \
  'StandardOutput=append:/opt/robo-vgt/logs/systemd_fechamento_mensal.log' \
  "StandardOutput=append:${INSTALL_DIR}/logs/systemd_fechamento_mensal.log"
replace_in_file "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.service" \
  'StandardError=append:/opt/robo-vgt/logs/systemd_fechamento_mensal.log' \
  "StandardError=append:${INSTALL_DIR}/logs/systemd_fechamento_mensal.log"

if [ "${INSTALL_SYSTEMD}" -eq 1 ]; then
  sudo cp "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.service" /etc/systemd/system/
  sudo cp "${INSTALL_DIR}/deploy/systemd/robo-pagos-novas-competencia-anterior.timer" /etc/systemd/system/
  if [ "${INSTALL_MONTHLY}" -eq 1 ]; then
    sudo cp "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.service" /etc/systemd/system/
    sudo cp "${INSTALL_DIR}/deploy/systemd/robo-fechamento-mensal.timer" /etc/systemd/system/
  fi
  sudo systemctl daemon-reload
  sudo systemctl enable --now robo-pagos-novas-competencia-anterior.timer
  if [ "${INSTALL_MONTHLY}" -eq 1 ]; then
    sudo systemctl enable --now robo-fechamento-mensal.timer
  fi
fi

echo
echo "Instalacao concluida."
echo
echo "Comandos uteis:"
echo "- Teste manual:"
echo "  sudo -u ${SERVICE_USER} ${INSTALL_DIR}/.venv/bin/python ${INSTALL_DIR}/pagos_novas_competencia_anterior.py --nome \"BONS NEGOCIOS\""
echo "- Rodar service agora:"
echo "  sudo systemctl start robo-pagos-novas-competencia-anterior.service"
echo "- Rodar fechamento mensal agora:"
echo "  sudo systemctl start robo-fechamento-mensal.service"
echo "- Ver logs:"
echo "  tail -f ${INSTALL_DIR}/logs/systemd_pagos_novas.log"
echo "  tail -f ${INSTALL_DIR}/logs/systemd_fechamento_mensal.log"
echo "- Subir segredos por FTPS/SFTP, se quiser manter fora do Git:"
echo "  ${INSTALL_DIR}/segredos/robo.local.json"
echo "  ${INSTALL_DIR}/segredos/imobiliarias.local.json"
