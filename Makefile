export PYTHONPATH := src

.PHONY: backfill-s3 up down logs producer batch silver gold pipeline dashboard reset run-bg stop-bg status-bg

up:            ## sobe os 3 brokers + cria tópico + kafka-ui
	podman compose up -d
	@echo "kafka-ui: http://localhost:8080"

down:          ## derruba os containers
	podman compose down

reset:         ## derruba e apaga o estado do kafka (usar se der ruim no boot)
	podman compose down -v

logs:
	podman compose logs -f kafka1 kafka2 kafka3

producer:      ## inicia a ingestão (Binance -> Kafka)
	uv run python src/producer.py

batch:         ## consumer batch (Kafka -> bronze)
	uv run python src/consumer_batch.py

silver:        ## bronze -> candles 1m
	uv run python src/transform/silver.py

gold:          ## silver -> métricas diárias
	uv run python src/transform/gold.py

pipeline: silver gold   ## roda silver + gold em sequência

dashboard:     ## streamlit (aba análise + aba ao vivo)
	uv run streamlit run dashboard/app.py

run-bg:        ## sobe producer + batch em background (nohup), sobrevive fechar o terminal
	mkdir -p logs
	nohup uv run python -u src/producer.py > logs/producer.log 2>&1 & echo $$! > .producer.pid
	nohup uv run python -u src/consumer_batch.py > logs/batch.log 2>&1 & echo $$! > .batch.pid
	@sleep 1
	@echo "rodando em background -- logs em logs/producer.log e logs/batch.log"
	@$(MAKE) status-bg

stop-bg:       ## para o producer + batch que estão rodando em background
	-pkill -f "src/producer.py"
	-pkill -f "src/consumer_batch.py"
	-rm -f .producer.pid .batch.pid
	@echo "producer/batch parados (offsets já commitados ficam salvos, sem perda de dado)"

backfill-s3:   ## sobe o bronze local pro S3 (idempotente, fase AWS)
	uv run python scripts/backfill_s3.py

status-bg:     ## mostra se producer/batch em background estão rodando + tamanho do bronze
	@pgrep -af "src/producer.py|src/consumer_batch.py" || echo "nada rodando em background"
	@find data/bronze -name '*.parquet' 2>/dev/null | wc -l | xargs echo "arquivos no bronze:"
