# Arquitetura

## Objetivo

Montar um motor 24/7 para identificar novos lancamentos e movimentos relevantes em IA, classificar materialidade, traduzir isso em tese de mercado e opcionalmente enviar ordens.

## Camadas

### 1. Ingestao

- `rss`: blogs oficiais, investor relations, midia setorial
- `json_api`: agregadores, buscas, APIs de noticias
- `POST /ingest`: conectores externos, crawlers headless, parceiros internos
- `CollectorWorker`: captura e empilha documentos novos em fila

### 2. Normalizacao

- `SourceDocument` padroniza titulo, URL, corpo, timestamps e tags
- fingerprint para dedupe
- estado por fonte salvo em storage configuravel
- fila configuravel entre coleta e inteligencia

### 3. Analise barata

- extracao heuristica de:
  - empresa
  - categoria
  - novidade
  - impacto potencial
  - temas
  - confianca

### 4. Escalonamento para LLM

Somente se:

- a heuristica esta incerta
- o impacto e alto
- a novidade parece alta
- ainda existe budget diario

### 5. Decisao

- `ImpactEngine` usa:
  - porte da empresa emissora
  - intensidade da inovacao
  - regras do impact graph
  - alvo em equity ou crypto
- `DecisionEngine` transforma assessments em ordens candidatas
- `RiskManager` filtra por:
  - confianca minima
  - impacto minimo
  - threshold mais alto para crypto
  - tamanho maximo
  - exposicao por ticker
  - exposicao total
  - cooldown
  - limite diario de ordens

### 6. Entrega da ordem

- `OutboxOrderDelivery` grava cada ordem aprovada em JSONL
- a integracao com corretora fica separada para ser adicionada depois

## Backends

- Storage:
  - `sqlite` por padrao
  - `postgres` suportado via `POSTGRES_DSN`
- Queue:
  - `sqlite` por padrao
  - `redis` suportado via `REDIS_URL`

## Resiliencia

- falha por fonte nao derruba o ciclo inteiro
- cada fonte recebe backoff proprio
- o sistema falha fechado se a entrega de ordem estiver mal configurada
- `dry_run` continua disponivel como trava operacional, mas a saida atual e apenas a outbox

## Evolucao recomendada

- rodar `collector`, `intelligence` e `delivery` como processos independentes
- trocar heuristica de novidade por embeddings + clusterizacao
- adicionar snapshots de pagina e features numericas para auditoria
- calibrar impact graph com benchmark historico
