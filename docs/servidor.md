# Rodar no servidor

## O que foi criado

- `pagos_novas_competencia_anterior.py`
  - baixa `pagos` e `novas locacoes` do dia `01` ate hoje
  - envia ao banco com competencia forcada para o mes anterior
- `fechamento_mensal.py`
  - baixa `pagos`, `nao_pagos` e `novas locacoes` do mes anterior
  - envia tudo para o banco
- `deploy/setup_server.sh`
  - instalador interativo para Linux
  - cria venv, instala dependencias, gera `.env.server`
  - opcionalmente instala `systemd service` e `timer`
  - opcionalmente instala tambem o fechamento mensal completo
- `deploy/systemd/robo-pagos-novas-competencia-anterior.service`
- `deploy/systemd/robo-pagos-novas-competencia-anterior.timer`
- `deploy/systemd/robo-fechamento-mensal.service`
- `deploy/systemd/robo-fechamento-mensal.timer`

## Fluxo desse script

Exemplo em `09/04/2026`:

- busca:
  - `01/04/2026` ate `09/04/2026`
- envia no banco com competencia:
  - `2026-03`

Comando manual:

```bash
python3 pagos_novas_competencia_anterior.py
```

Filtrando uma imobiliaria:

```bash
python3 pagos_novas_competencia_anterior.py --nome "BONS NEGOCIOS"
```

## Fluxo do fechamento mensal

Exemplo em `01/04/2026`:

- baixa:
  - `01/03/2026` ate `31/03/2026`
- executa:
  - `pagos`
  - `nao_pagos`
  - `novas locacoes`
- envia tudo ao banco

Comando manual:

```bash
python3 fechamento_mensal.py
```

## Preparacao no Linux

### Modo mais simples

Suba a pasta do projeto no servidor e rode:

```bash
cd /caminho/do/projeto
chmod +x deploy/setup_server.sh
./deploy/setup_server.sh
```

Ele vai perguntar:

- pasta final do projeto
- usuario do sistema
- host, usuario, senha e nome do banco
- se deve instalar pacotes via `apt`
- se deve instalar o `systemd`
- se deve instalar tambem o timer de fechamento mensal

Depois disso, se quiser manter senhas fora do Git, suba por FTPS/SFTP:

- `/opt/robo-vgt/segredos/robo.local.json`
- `/opt/robo-vgt/segredos/imobiliarias.local.json`

O instalador cria esses arquivos com exemplo se eles ainda nao existirem.

### Modo manual

Exemplo assumindo projeto em `/opt/robo-vgt`.

```bash
cd /opt
python3 -m venv /opt/robo-vgt/.venv
/opt/robo-vgt/.venv/bin/pip install -r /opt/robo-vgt/requirements.txt
chmod +x /opt/robo-vgt/deploy/systemd/rodar_pagos_novas_competencia_anterior.sh
mkdir -p /opt/robo-vgt/logs
```

## Instalar no systemd

```bash
sudo cp /opt/robo-vgt/deploy/systemd/robo-pagos-novas-competencia-anterior.service /etc/systemd/system/
sudo cp /opt/robo-vgt/deploy/systemd/robo-pagos-novas-competencia-anterior.timer /etc/systemd/system/
sudo cp /opt/robo-vgt/deploy/systemd/robo-fechamento-mensal.service /etc/systemd/system/
sudo cp /opt/robo-vgt/deploy/systemd/robo-fechamento-mensal.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now robo-pagos-novas-competencia-anterior.timer
sudo systemctl enable --now robo-fechamento-mensal.timer
```

## Rodar sem esperar o horario

```bash
sudo systemctl start robo-pagos-novas-competencia-anterior.service
sudo systemctl start robo-fechamento-mensal.service
```

## Ver logs

```bash
tail -f /opt/robo-vgt/logs/systemd_pagos_novas.log
tail -f /opt/robo-vgt/logs/systemd_fechamento_mensal.log
journalctl -u robo-pagos-novas-competencia-anterior.service -f
journalctl -u robo-fechamento-mensal.service -f
```

## Observacoes

- O servidor precisa ter Chrome e ChromeDriver compativeis.
- Se o robo continuar usando `bin/chromedriver`, copie essa pasta junto.
- A pasta `segredos/` fica fora do Git e o instalador preserva essa pasta em novos deploys.
- As credenciais do banco podem continuar por variavel de ambiente:
  - `ROBO_DB_HOST`
  - `ROBO_DB_USER`
  - `ROBO_DB_PASSWORD`
  - `ROBO_DB_DATABASE`
- O script ja define automaticamente:
  - `ROBO_COMPETENCIA_FIXA`
  - `ROBO_COMPETENCIA_FORCADA`

## Recomendacao de servidor

Para esse projeto, o melhor alvo eh um Linux com interface grafica minima ou Chrome headless validado:

- Ubuntu 22.04 ou 24.04
- 4 vCPU
- 8 GB RAM
- disco SSD

Se quiser estabilidade operacional, eu recomendo:

1. subir o projeto num Ubuntu
2. validar uma execucao manual
3. ativar o timer diario
4. ativar tambem o timer do fechamento mensal
