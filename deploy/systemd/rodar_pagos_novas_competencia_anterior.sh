#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/opt/robo-vgt"
VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"

cd "${PROJECT_ROOT}"

if [ -f "${PROJECT_ROOT}/.env.server" ]; then
  set -a
  . "${PROJECT_ROOT}/.env.server"
  set +a
fi

if [ -x "${VENV_PYTHON}" ]; then
  exec "${VENV_PYTHON}" "${PROJECT_ROOT}/pagos_novas_competencia_anterior.py"
fi

exec python3 "${PROJECT_ROOT}/pagos_novas_competencia_anterior.py"
