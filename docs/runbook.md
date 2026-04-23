# Runbook

## Modo seguro inicial

- `dry_run = true`
- entrega `outbox`
- LLM desligado
- thresholds mais conservadores
- `sqlite` para storage e queue

## Check rapido

```bash
PYTHONPATH=src python3 -m ai_innovation_monitoring health
PYTHONPATH=src python3 -m ai_innovation_monitoring print-config
```

## Rodar um ciclo

```bash
PYTHONPATH=src python3 -m ai_innovation_monitoring run --once
```

## Rodar workers separados

```bash
PYTHONPATH=src python3 -m ai_innovation_monitoring worker collector --once
PYTHONPATH=src python3 -m ai_innovation_monitoring worker intelligence --once
PYTHONPATH=src python3 -m ai_innovation_monitoring worker delivery --once
```

## Rodar como servico HTTP

```bash
PYTHONPATH=src python3 -m ai_innovation_monitoring serve --host 0.0.0.0 --port 8080
```

## Dashboard

- UI: `http://localhost:8080/`
- snapshot agregado: `http://localhost:8080/dashboard-data`

## Healthcheck

```bash
curl http://localhost:8080/health
```

## Sinais de problema

- `last_error` repetindo a mesma fonte: URL mudou, feed quebrou, bloqueio, timeout
- `blocked_orders` alto demais: regras de risco muito restritivas ou sizing errado
- `llm_spend_last_24h` alto: thresholds frouxos
- `orders_outbox.jsonl` sem novas linhas: sem sinais aprovados ou falha na entrega da ordem
- filas crescendo em `/health`: collector mais rapido que intelligence/delivery

## Ajustes operacionais

- aumentar `poll_interval_seconds` para fontes menos criticas
- ampliar coverage por `/ingest` com scrapers externos
- elevar confianca minima para reduzir falsos positivos
- revisar o consumidor da outbox antes de ligar qualquer integracao real
- migrar `storage.kind=postgres` e `queue.kind=redis` antes de carga maior
