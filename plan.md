# Plano: rodar o Sentinela Coins ponta a ponta + tooling (uv/ruff/pytest/CI) + GitHub

## Contexto

O projeto é um esqueleto **gerado mas nunca executado** (`data/` só tem `.gitkeep`, sem bronze/silver/gold, sem git). Antes de qualquer refino, o objetivo (CLAUDE.md item 8) é fazer o pipeline rodar de ponta a ponta local. Na exploração e discussão prévias, identificamos e validamos com o usuário duas frentes:

**Frente A — rodar o pipeline (ambiente + fixes + forward-fill):**
1. A máquina não tem Docker, só **Podman** (`podman 5.8.4` + `podman-compose 1.6.0`, testados e funcionais — `podman compose` despacha pro `podman-compose` e lê o `docker-compose.yml` atual sem reestruturação). O Makefile chama `docker compose` direto, que falha aqui.
2. Gaps baratos de qualidade no skeleton (falta `.env.example`, sem callback de entrega no producer, tag `:latest` flutuante no kafka-ui, erros de consumer descartados sem log) — incluídos agora, não deixados pra depois.
3. `silver.py` hoje não gera candle pra minutos sem trade (gap) — implementar **forward-fill** (preço repete, volume/trades zeram).
4. Janela do candle continua fixa em 1 minuto. Fonte de dado continua Binance (só migra se o WS falhar de fato — geoblock).

**Frente B — tooling e CI (pedido nesta rodada):**
5. Gerenciar o ambiente Python com **uv** (`pyproject.toml` + `uv.lock`, substituindo `requirements.txt`/venv manual — jeito idiomático, dá lockfile reprodutível e `uv run` cuida do venv sozinho).
6. **ruff** pra lint/format, com hook de **pre-commit** local (rápido, só ruff — não trava commit com pytest).
7. **pytest** com um punhado de testes de lógica pura (sem subir Kafka/Podman no CI): `build_url()` do producer, particionamento `symbol/dt` do `flush()` no consumer_batch, e principalmente a query de **forward-fill do silver.py** (parquet sintético com buraco de minuto → valida candle flat) e as métricas do gold.py.
8. **GitHub Actions** simples: PR dispara `ruff check` + `ruff format --check` + `pytest` (gate de aprovação do PR).
9. `git init` + `.gitignore` cuidadoso (fora do controle de versão: `data/*` exceto `.gitkeep`, `.venv/`, `.env`) + criar repo **público** novo via `gh repo create` + primeiro push.

Ambiente já verificado: `uv 0.11.24` e `gh 2.97.0` (autenticado como `romualdobarbosa`, protocolo SSH) instalados e funcionais.

Resultado esperado: pipeline rodando local ponta a ponta, ambiente gerenciado por uv, lint/testes automatizados, código versionado e publicado num repo público no GitHub com CI rodando em cada PR.

---

## Frente A — Mudanças no pipeline

### 1. `Makefile` — Docker → Podman (e depois uv, ver Frente B #6)
Trocar `docker compose` por `podman compose` nos targets `up`, `down`, `reset`, `logs`.

### 2. `docker-compose.yml` — pin do kafka-ui
`image: kafbat/kafka-ui:latest` → `image: kafbat/kafka-ui:v1.5.0` (tag de release real). Resto do compose não muda — já validado que `podman compose` lê a estrutura KRaft/3 brokers/kafka-init sem reestruturação.

### 3. `README.md` — coerência
Atualizar menções a "Docker/Docker Compose" pra Podman. Atualizar setup pra usar uv (ver Frente B).

### 4. Novo `.env.example`
Espelhar as chaves/defaults de `src/config.py` (`SYMBOLS`, `KAFKA_BOOTSTRAP`, `TOPIC`, `BINANCE_WS_BASE`, `BRONZE_PATH`/`SILVER_PATH`/`GOLD_PATH`, `BATCH_MAX_MESSAGES`, `BATCH_MAX_SECONDS`), resolvendo o `cp .env.example .env` do README que hoje quebra.

### 5. `src/producer.py` — callback de entrega
Função `delivery_report(err, msg)` logando (`print`) quando `err is not None`; passar `callback=delivery_report` em `producer.produce(...)` (linha 46). `producer.poll(0)` já dispara o callback.

### 6. `src/consumer_batch.py` — logar erro do consumer
No loop `run()` (linha 62-64), logar `msg.error()` em vez de descartar silenciosamente, sem mudar commit/flush.

### 7. `dashboard/live.py` — logar erro do consumer
Mesmo padrão em `poll_trades()` (linha 33-34).

### 8. `src/transform/silver.py` — forward-fill de minutos sem trade
Reescrever a query dentro de `build()`, mantendo assinatura e `COPY (...) TO ...` externo:
- `real_candles`: agregação atual (open/high/low/close/volume/quote_volume/trades por symbol+minute), sem mudança de lógica.
- `bounds`: min/max minuto por símbolo.
- `minute_grid`: grade completa via `generate_series(min_minute, max_minute, INTERVAL '1 minute')` + `unnest`.
- `LEFT JOIN` da grade com `real_candles`.
- `LAST_VALUE(close IGNORE NULLS) OVER (PARTITION BY symbol ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` pra achar o último close real; minutos sintéticos: `open=high=low=close=last_close`, `volume/quote_volume/trades=0`.
- Primeiro minuto de cada símbolo nunca é sintético (vem de `bounds`), então `last_close` nunca é NULL na primeira linha da partição.

**Efeito colateral aceito**: `n_candles` no `gold.py` passa a contar minutos de sessão totais (incluindo flat), não só com negociação real. `vwap`/`volume`/`quote_volume` do gold não são afetados. Não requer mudança no `gold.py`.

---

## Frente B — uv, qualidade, testes, CI e GitHub

### 9. Migração pra uv
- `uv init --no-readme` (ou criação manual do `pyproject.toml`) preservando nome/estrutura do projeto, `requires-python = ">=3.11"`.
- `uv add confluent-kafka websockets polars duckdb streamlit streamlit-autorefresh python-dotenv` (recriando as mesmas dependências do `requirements.txt`, deixando o uv escrever as constraints e gerar `uv.lock`).
- `uv add --dev ruff pytest pre-commit`.
- Remover `requirements.txt` (substituído por `pyproject.toml` + `uv.lock`).
- `Makefile`: prefixar os targets Python com `uv run` (`uv run python src/producer.py`, `uv run streamlit run dashboard/app.py`, etc.); setup vira `uv sync` no lugar de `python -m venv` + `pip install -r requirements.txt`.

### 10. `ruff`
Config em `[tool.ruff]` no `pyproject.toml`: `line-length = 100`, `target-version = "py311"`, `[tool.ruff.lint] select = ["E", "F", "I", "UP", "B"]` (regras padrão sensatas, sem exagero pra portfólio).

### 11. `.pre-commit-config.yaml`
Hook único do `ruff-pre-commit` (checar a tag mais recente publicada no momento da implementação, não chutar versão) rodando `ruff check --fix` e `ruff format`. Pytest **não** entra no pre-commit (decisão do usuário) — só no CI.

### 12. Testes pytest (`tests/`)
Sem subir Kafka/Podman — só lógica pura, usando `tmp_path`/fixtures do pytest e parquet sintético via DuckDB/Polars direto no teste:
- `tests/test_producer.py`: `build_url()` gera a URL combinada certa a partir de uma lista de símbolos.
- `tests/test_consumer_batch.py`: `flush()` particiona corretamente em `symbol=<>/dt=<>/` dado um buffer de mensagens fake (monkeypatch em `BRONZE_PATH` apontando pro `tmp_path`).
- `tests/test_silver.py`: escreve um parquet sintético no bronze com um buraco de minuto proposital, roda `build()`, valida que (a) não sobra gap de minuto na saída e (b) o(s) minuto(s) sintético(s) tem `trades=0`, `volume=0`, `open=high=low=close` = close do candle real anterior — é o teste de regressão da mudança #8.
- `tests/test_gold.py`: parquet sintético de candles conhecidos, valida `vwap`, `range_pct`, `change_pct` calculados à mão.
- `pyproject.toml`: `[tool.pytest.ini_options] testpaths = ["tests"]` e `pythonpath = ["src"]` (dispensa depender de `PYTHONPATH` externo tanto local quanto no CI).

### 13. `.github/workflows/ci.yml`
Dispara em `pull_request` (e opcionalmente push em `main`):
```yaml
- actions/checkout@v4
- astral-sh/setup-uv@v3 (com cache habilitado)
- uv sync
- uv run ruff check .
- uv run ruff format --check .
- uv run pytest
```
Sem serviços de Kafka/Postgres no CI — os testes são desenhados pra não precisar.

### 14. `.gitignore`
```
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.venv/
.env
data/*
!data/.gitkeep
```
(`uv.lock` **não** entra no gitignore — é commitado, é o lockfile reprodutível.)

### 15. `git init` + primeiro push
- `git init`, criar `.gitignore` (#14) antes do primeiro `add`.
- Revisar `git status` pra confirmar que `.env`, `.venv/` e conteúdo de `data/` (exceto `.gitkeep`) não aparecem antes de commitar.
- `git add` + commit inicial.
- `gh repo create sentinela_coins --public --source=. --remote=origin` (repo novo, público, conta já autenticada `romualdobarbosa`).
- `git push -u origin main`.

---

## Ordem de execução geral

1. **Frente B primeiro (tooling)**: uv (#9) → ruff (#10) → pre-commit (#11) → testes (#12, incluindo o teste do forward-fill *antes* de implementar a mudança #8, validando que ele falha no código atual e passa depois — TDD leve) → CI workflow (#13, só valida sintaticamente até existir remoto).
2. **Frente A (pipeline)**: Podman (#1-2) → fixes (#4-7) → forward-fill (#8) → rodar teste `tests/test_silver.py` localmente confirmando que passa.
3. **git init + push (#14-15)**: só depois do que já estiver rodando localmente (bronze/silver/gold gerados) — assim o primeiro commit já reflete um estado testado, mas lembrando que `data/` fica de fora do repo (só o código é versionado).
4. Validação E2E completa (Podman up → producer/batch → pipeline → dashboard), conforme critérios abaixo.
5. Abrir um PR de teste (branch separada) pra confirmar que a esteira do GitHub Actions dispara e passa — valida a CI de ponta a ponta.

## Critérios de verificação (E2E local, pipeline)

- `make up` (via podman): `podman ps` com kafka1/2/3 `Up`; log do `kafka-init` com tópico 6 partições/RF=3; kafka-ui em `localhost:8080` com 3 brokers online.
- `make producer` em foreground ~10-15s: conecta sem loop de reconexão a cada 5s (geoblock apareceria assim).
- `make producer` + `make batch` por alguns minutos: parquet aparecendo em `data/bronze/symbol=<>/dt=<>/`, sem erro inesperado logado.
- `make pipeline`: silver sem gaps de minuto (query de `LAG` por símbolo checando `gap > INTERVAL '1 minute'` retorna zero linhas); gold com 1 linha por symbol/dia e métricas coerentes.
- `make dashboard`: aba "Análise" carrega tabela+gráfico; aba "Ao vivo" atualiza a cada 2s sem erro repetido no terminal.
- `uv run pytest`: todos os testes verdes, incluindo o de forward-fill.
- `uv run ruff check . && uv run ruff format --check .`: sem violações.
- PR de teste no GitHub: Actions dispara e os 3 steps (ruff check, ruff format, pytest) passam.

## Arquivos afetados/criados
- `Makefile`, `docker-compose.yml`, `README.md`
- `.env.example` (novo)
- `src/producer.py`, `src/consumer_batch.py`, `dashboard/live.py`, `src/transform/silver.py`
- `pyproject.toml` (novo), `uv.lock` (novo, gerado), remove `requirements.txt`
- `.pre-commit-config.yaml` (novo)
- `tests/test_producer.py`, `tests/test_consumer_batch.py`, `tests/test_silver.py`, `tests/test_gold.py` (novos)
- `.github/workflows/ci.yml` (novo)
- `.gitignore` (novo)

---

## Status de execução (atualizado após implementação)

Tudo acima foi implementado e commitado. Ajustes feitos durante a execução, além do previsto neste plano:

- **Resolução de nome de imagem no Podman**: `/etc/containers/registries.conf` tem `short-name-mode = "enforcing"`, então `podman compose up` falhava com "short-name resolution enforced but cannot prompt without a TTY" pras imagens `confluentinc/cp-kafka` e `kafbat/kafka-ui` (sem registry explícito). Corrigido qualificando as imagens no `docker-compose.yml` com prefixo `docker.io/` (mais correto que mexer em config global do sistema).
- **Bug real no `kafka-init`**: o `command: >` (YAML folded scalar) tinha uma linha do `kafka-topics --create` mais indentada que as demais, o que faz o YAML preservar uma quebra de linha literal ali em vez de dobrar em espaço — o shell interpretava isso como dois comandos separados (`--create --if-not-exists` sem `--topic`, depois `--topic ...` como comando inexistente). Corrigido colocando toda a linha do `--create` na mesma indentação das demais, virando uma única linha de comando.
- Repo publicado em `https://github.com/romualdobarbosa/sentinela_coins`, CI validado com sucesso no push inicial pro `main` (ruff check + ruff format + pytest, todos verdes).
- Cluster Kafka via Podman validado: 3 brokers `Up`, tópico `crypto.trades` com 6 partições/RF=3, todas com 3 réplicas no ISR; kafka-ui reportando cluster `ONLINE`.
- Conectividade real com a Binance confirmada (sem geoblock nesse ambiente) via `make producer` em foreground.
- Producer + `consumer_batch` rodaram ~5min em background, acumulando ~9600 trades reais nas 5 moedas, sem erro.
- **Bug real encontrado no `silver.py` durante a validação com dado real**: o DuckDB resolve nomes de coluna sem diferenciar maiúscula/minúscula, mas o bronze tem colunas distintas `t` (trade id) e `T` (trade time) vindas do JSON cru da Binance — ao ler `T` via SQL, o DuckDB colapsava silenciosamente pra `t`, fazendo os candles usarem o trade ID como se fosse timestamp (candles saíam com datas de 1970). Achado comparando uma leitura via Polars (case-sensitive, correta) com a mesma leitura via DuckDB glob (case-insensitive, errada). Corrigido fazendo `silver.py` ler as colunas `price`/`qty`/`trade_time` que o `consumer_batch.py` já grava tipadas no bronze (via Polars, sem ambiguidade), em vez de recalcular a partir de `p`/`q`/`T` crus. Teste `test_silver.py` também ajustado pra gerar o bronze fixture via `consumer_batch.flush()` real (em vez de um parquet escrito à mão), fechando esse buraco de cobertura entre bronze e silver.
- Pipeline reprocessado com o fix: `make pipeline` gerou candles com datas corretas (2026-09-16), sem buracos de minuto, métricas de gold plausíveis (ex: BTCUSDT variação +0,057% na janela coletada).
- Validado `dashboard/app.py` via `streamlit.testing.v1.AppTest` (headless, sem browser): aba "Análise" renderiza a tabela/gráfico do gold sem exceção; aba "Ao vivo" cai corretamente no fallback "sem trades ainda" quando não há producer ativo no instante do teste (esperado — consumer group precisa de um rebalance antes de receber mensagens, e a UI real reforça isso via autorefresh de 2s). De passagem, corrigido um aviso de depreciação do Streamlit (`use_container_width=True` → `width="stretch"`).
