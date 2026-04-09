# Robo VGT

Automacao para baixar planilhas de:

- `pagos`
- `nao_pagos`
- `novas locacoes`

e subir os dados para o banco.

## Fluxos principais

- `python3 fechamento_mensal.py`
  - roda `pagos`, `nao_pagos` e `novas locacoes` do mes anterior
- `python3 pagos_novas_competencia_anterior.py`
  - roda `pagos` e `novas locacoes` do dia 01 ate hoje
  - envia no banco com competencia do mes anterior

## Automacao no servidor

O projeto ja vem com:

- instalador interativo:
  - `deploy/setup_server.sh`
- rotina diaria no `systemd`:
  - `robo-pagos-novas-competencia-anterior.service`
  - `robo-pagos-novas-competencia-anterior.timer`
- fechamento mensal no `systemd`:
  - `robo-fechamento-mensal.service`
  - `robo-fechamento-mensal.timer`

## Publicar no GitHub

Antes de publicar, revise credenciais e acessos sensiveis.

Pontos que hoje exigem atencao:

- `app/config_banco.py`
- `app/robo_core.py`
- arquivos em `app/imobiliarias/` que tenham `login` e `senha`

Se o repositorio for publico, o ideal eh remover senhas fixas do codigo e usar apenas variaveis de ambiente.

## Segredos fora do Git

O projeto agora suporta credenciais locais fora do repositorio:

- `segredos/robo.local.json`
  - login padrao do robo
  - template da senha padrao
  - chave da API
- `segredos/imobiliarias.local.json`
  - logins e senhas especificas por imobiliaria

Esses arquivos ficam fora do Git por causa do `.gitignore`.
No servidor, voce pode subir essa pasta por FTPS/SFTP depois do clone.

## Instalar no servidor

Suba o projeto no GitHub e depois, no servidor Linux:

```bash
git clone <URL_DO_REPOSITORIO> /opt/robo-vgt
cd /opt/robo-vgt
chmod +x deploy/setup_server.sh
./deploy/setup_server.sh
```

## Rodar o instalador direto pela internet

Se o repositorio estiver no GitHub, voce pode usar a URL raw do script:

```bash
curl -fsSL https://raw.githubusercontent.com/SEU_USUARIO/SEU_REPO/main/deploy/setup_server.sh -o setup_server.sh
chmod +x setup_server.sh
./setup_server.sh
```

## Recomendacao

O jeito mais seguro eh:

1. subir primeiro em um repositorio privado
2. testar o servidor
3. so depois decidir se quer abrir o repositorio

## Documentacao adicional

- [docs/servidor.md](/Users/lucasmoraes/Projects/Robo%20-%20VGT/docs/servidor.md)
