"""Config central. Tudo parametrizável por .env (ver .env.example)."""

import os

from dotenv import load_dotenv

load_dotenv()

# Pares que o producer vai assinar (minúsculo, formato Binance)
SYMBOLS = os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt").split(",")

# Brokers expostos no host pelo docker-compose (3 brokers)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
TOPIC = os.getenv("TOPIC", "crypto.trades")

# Endpoint só de market data (sem auth). Combined stream.
BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision/stream")

# Camadas medallion. Troca BRONZE_PATH por s3://seu-bucket/bronze na fase AWS
# (silver/gold continuam locais -- só o bronze prova a integração com a cloud).
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "./data/gold")

# Só usadas quando BRONZE_PATH é s3://... (fase AWS). Vazias = comportamento local intacto.
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# Política de flush do consumer batch: grava quando bater um dos dois
BATCH_MAX_MESSAGES = int(os.getenv("BATCH_MAX_MESSAGES", "500"))
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS", "30"))
