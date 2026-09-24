"""Sobe o bronze local pro S3 preservando o layout symbol=<>/dt=<>/<ts>.parquet.

Fase AWS: backfill dos dias já ingeridos localmente. Idempotente -- só envia o que
ainda não está no bucket (mesmo caminho e mesmo tamanho), então dá pra rodar de novo
se cair no meio. No final confere contagem e bytes local vs S3.

Uso: make backfill-s3   (precisa de AWS_* e S3_BUCKET no .env)
"""

import os
import sys
from pathlib import Path

import s3fs

from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, BRONZE_PATH

LOCAL_ROOT = Path(BRONZE_PATH)
BUCKET = os.getenv("S3_BUCKET", "")


def main() -> None:
    if not BUCKET:
        sys.exit("defina S3_BUCKET no .env (só o nome do bucket, sem s3://)")
    if LOCAL_ROOT.as_posix().startswith("s3:"):
        sys.exit(
            "BRONZE_PATH já aponta pro S3; o backfill lê do bronze local -- volte pra ./data/bronze"
        )

    fs = s3fs.S3FileSystem(
        key=AWS_ACCESS_KEY_ID,
        secret=AWS_SECRET_ACCESS_KEY,
        client_kwargs={"region_name": AWS_REGION},
    )
    remote_root = f"{BUCKET}/bronze"

    local = {
        p.relative_to(LOCAL_ROOT).as_posix(): p.stat().st_size
        for p in LOCAL_ROOT.rglob("*.parquet")
    }
    remote = (
        {
            k.removeprefix(f"{remote_root}/"): v["size"]
            for k, v in fs.find(remote_root, detail=True).items()
        }
        if fs.exists(remote_root)
        else {}
    )
    todo = [rel for rel, size in local.items() if remote.get(rel) != size]
    print(
        f"[backfill] local: {len(local)} arquivos | já no S3: {len(local) - len(todo)} | "
        f"a enviar: {len(todo)}"
    )

    if todo:
        fs.put(
            [str(LOCAL_ROOT / rel) for rel in todo],
            [f"{remote_root}/{rel}" for rel in todo],
            batch_size=32,
        )

    remote = {
        k.removeprefix(f"{remote_root}/"): v["size"]
        for k, v in fs.find(remote_root, detail=True).items()
    }
    ok = {rel for rel, size in local.items() if remote.get(rel) == size}
    print(
        f"[backfill] S3: {len(remote)} arquivos, {sum(remote.values()) / 1e6:.1f} MB | "
        f"local: {len(local)} arquivos, {sum(local.values()) / 1e6:.1f} MB"
    )
    if len(ok) != len(local):
        sys.exit(f"[backfill] FALHOU: {len(local) - len(ok)} arquivos divergentes")
    print("[backfill] ok: contagem e tamanhos batem")


if __name__ == "__main__":
    main()
