# AI Innovation Monitoring

Projeto para monitoramento 24/7 de inovações e lançamentos de empresas de IA e avaliação de influência positiva/negativa em setores e empresas do mercado com um sistema de decisões inteligente.

## O que este projeto entrega

- Coleta continua por fontes de alta densidade de sinal: RSS/Atom, APIs JSON e sitemaps oficiais.
- Pipeline em camadas: coleta -> fila -> analise heuristica -> escalonamento seletivo para LLM -> impact graph -> decisao -> risco -> entrega da ordem.
- Base de dados e fila configuraveis:
  - `sqlite` por padrao
  - suporte opcional a `postgres` para storage
  - suporte opcional a `redis` para queue
- Registry de empresas e impact graph:
  - `company_registry` com porte e aliases
  - `impact_graph` para mapear inovacao -> empresas listadas/cripto afetadas
- Economia de credito de LLM:
  - heuristica primeiro
  - LLM so quando ha alta novidade, alto impacto ou baixa confianca
  - teto diario de gasto
- Operacao mais resiliente:
  - backoff por fonte
  - rate limiting por host
  - uso preferencial de feeds/APIs para reduzir bloqueio
  - endpoint `/ingest` para receber dados de crawlers externos
- Entrega da ordem pronta para:
  - `outbox` JSONL local, para integracao posterior com sua corretora/OMS
- API HTTP para healthcheck, eventos, ordens e ingestao externa.

## Limite importante

Nao existe "monitorar toda a internet" de forma literal sem infraestrutura propria de busca/crawling, custos altos e riscos operacionais. O desenho correto e:

1. Cobrir 80/20 com fontes oficiais e midia especializada via feeds/APIs.
2. Enriquecer com crawlers externos dedicados, que empurram documentos para `POST /ingest`.
3. Escalonar para browser/scraping pesado so onde houver lacuna material.

Este repositorio entrega o motor central e a interface de integracao para essa arquitetura.

## Arquitetura

```text
Sources (RSS / JSON API / sitemap / external webhook)
  -> Collector worker
  -> Queue
  -> Intelligence worker
  -> HeuristicAnalyzer
  -> CostAwareRouter
  -> OpenAI-compatible LLM analyzer (opcional)
  -> ImpactEngine
  -> DecisionEngine
  -> RiskManager
  -> Delivery worker
  -> Order outbox
  -> HTTP API / storage / observability
```

Arquivos principais:

- [src/ai_innovation_monitoring/orchestrator.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/orchestrator.py)
- [src/ai_innovation_monitoring/analysis.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/analysis.py)
- [src/ai_innovation_monitoring/impact.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/impact.py)
- [src/ai_innovation_monitoring/decision.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/decision.py)
- [src/ai_innovation_monitoring/queueing.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/queueing.py)
- [src/ai_innovation_monitoring/storage.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/storage.py)
- [src/ai_innovation_monitoring/order_delivery.py](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/src/ai_innovation_monitoring/order_delivery.py)
- [config/sources.json](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/config/sources.json)
- [config/market_profile.json](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/config/market_profile.json)
- [config/company_registry.json](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/config/company_registry.json)
- [config/impact_graph.json](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/config/impact_graph.json)

## Como rodar local

### Opcao 1: local sem instalar

```bash
PYTHONPATH=src python3 -m ai_innovation_monitoring health
PYTHONPATH=src python3 -m ai_innovation_monitoring run --once
PYTHONPATH=src python3 -m ai_innovation_monitoring worker collector --once
PYTHONPATH=src python3 -m ai_innovation_monitoring worker intelligence --once
PYTHONPATH=src python3 -m ai_innovation_monitoring worker delivery --once
PYTHONPATH=src python3 -m ai_innovation_monitoring serve --host 0.0.0.0 --port 8080
```

Abra `http://localhost:8080/` para o dashboard terminal em tempo real.

### Opcao 2: instalando o pacote

```bash
python3 -m pip install -e .
ai-monitor health
ai-monitor run --once
ai-monitor worker collector --once
ai-monitor serve --host 0.0.0.0 --port 8080
```

## Configuracao

O projeto ja inclui `config/app.local.json` em modo seguro:

- `dry_run: true`
- entrega em `outbox`
- LLM desabilitado
- `company_registry` e `impact_graph` carregados no bootstrap com arquivos reais em `config/*.json`

Arquivos:

- [config/app.local.json](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/config/app.local.json)
- [.env.template](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/.env.template)

Variaveis:

- `OPENAI_API_KEY`
- `POSTGRES_DSN`
- `REDIS_URL`
- `INGEST_TOKEN`

## Estrategia para economizar LLM

- Cada documento passa primeiro por heuristica barata.
- O roteador so chama LLM quando:
  - novidade >= threshold
  - impacto de mercado >= threshold
  - incerteza alta
- Gasto de LLM e registrado em banco e respeita budget diario.
- Se o budget acabar, o sistema continua em modo heuristico.

## Estrategia anti-bloqueio

- Preferencia por RSS/Atom, sitemaps oficiais e APIs publicas.
- `If-None-Match` e `If-Modified-Since` quando disponiveis.
- Rate limit por host.
- Backoff exponencial por fonte em erro/429/5xx.
- Endpoint webhook para acoplar crawlers distribuidos sem concentrar scraping aqui.
- Quando uma fonte oficial bloqueia fetch simples ou depende de JS, o coletor pode usar Selenium headless em modo `browser`.
- Fontes sem feed estavel, como newsroom/paginas de categoria que mudam com frequencia, podem entrar por crawler externo usando `POST /ingest`.

## Endpoints

- `GET /health`
- `GET /`
- `GET /dashboard`
- `GET /dashboard-data`
- `GET /events?limit=20`
- `GET /orders?limit=20`
- `GET /registry?limit=100`
- `GET /impact-assessments?limit=50`
- `POST /run-once`
- `POST /ingest`

Exemplo de ingestao externa:

```bash
curl -X POST http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -H 'X-Ingest-Token: change-me' \
  -d '{
    "source_name": "google-deepmind-blog",
    "url": "https://blog.google/innovation-and-ai/technology/ai/lyria-3-pro/",
    "title": "Lyria 3 Pro: Create longer tracks in more Google products",
    "content": "We are bringing Lyria 3 to the tools where professionals work and create every day.",
    "tags": ["official", "google", "ai"]
  }'
```

## Saida de ordem

As ordens aprovadas pelo motor sao emitidas em:

- [data/orders_outbox.jsonl](/home/lucas/Área%20de%20trabalho/AI-inovation-monitoring/data/orders_outbox.jsonl)

Cada linha contem uma ordem estruturada pronta para sua futura integracao.

## Fontes e decisao

O projeto agora separa tres camadas de inteligencia:

- `market_profile`: heuristica barata por tema e aliases
- `company_registry`: tamanho e identidade das empresas monitoradas
- `impact_graph`: como uma inovacao tende a afetar ativos listados e cripto em casos raros

Os arquivos canônicos devem refletir o universo real monitorado. Em producao, o correto e:

- modelar teses por subsetor
- mapear competidores publicos vs privados
- calibrar `size_score` e `base_weight` com historico
- validar liquidez, borrow, slippage e compliance
- separar claramente sinais de noticia, execucao e portfolio construction

## Proximos passos recomendados

1. Popular `company_registry` e `impact_graph` com cobertura real do universo monitorado.
2. Ligar `postgres` e `redis` quando sair do modo local.
3. Integrar provedores de busca/crawling externos via `/ingest`.
4. Colocar observabilidade com Prometheus/Grafana/Sentry.
5. Implementar aprovacao humana para eventos acima de certo impacto.

## Aviso

Este projeto e base tecnica, nao recomendacao financeira. A decisao automatica de ordens deve passar por validacao juridica, compliance, risco e testes antes de qualquer uso real.
