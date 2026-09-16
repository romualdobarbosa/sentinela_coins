export PYTHONPATH := src

.PHONY: up down logs producer batch silver gold pipeline dashboard reset

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
