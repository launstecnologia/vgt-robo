# Arquitetura de Pastas

## Estrutura principal

```text
Robo - VGT/
  app/
    config_banco.py
    gerar_sql.py
    insert_banco.py
    robo_core.py
    imobiliarias_eventos.py
    imobiliarias/
  scripts/
    debug_listar_eventos.py
    run_repetir_falhas.py
  data/
    locatarios_gerados.json
  downloads/
  logs/
  bin/
  main.py
  main_pagos.py
  main_nao_pagos.py
  main_novas_locacoes.py
  main_mbrokers.py
  envioapi.py
  envioapi_novas_locacoes.py
```

## Regras

- `app/`: codigo principal do projeto
- `app/imobiliarias/`: configuracao individual de cada imobiliaria
- `scripts/`: utilitarios e manutencao
- `data/`: arquivos gerados pelo processamento
- `downloads/`: planilhas baixadas pelo robo
- `logs/`: logs de execucao e erro
- `bin/`: executaveis locais, como `chromedriver`
- raiz do projeto: pontos de entrada para execucao manual
