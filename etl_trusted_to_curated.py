"""
Promove registros do bucket Trusted (silver) para Curated (gold) e insere
os registros limpos no banco de dados MySQL quando possível.

Usage:
  Configure variáveis de ambiente: BUCKET_TRUSTED (ou BUCKET_SILVER),
  BUCKET_CURATED (ou BUCKET_GOLD), S3_ENDPOINT opcional, ou deixe
O script tenta inserir em uma tabela `medicao` com colunas
  (id_farm, latencia_pool, latencia_dns, latencia_gateway, timestamp_medicao)
Se a tabela não existir ele continuará sem falhar.
"""

import json
import os
from datetime import datetime

import boto3
import tempfile
import mysql.connector

BUCKET_TRUSTED = os.environ.get("BUCKET_TRUSTED") or os.environ.get("BUCKET_SILVER")
BUCKET_CURATED = os.environ.get("BUCKET_CURATED") or os.environ.get("BUCKET_GOLD")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
LOCAL_BUCKET_DIR = os.environ.get("LOCAL_BUCKET_DIR", "./bucket_raw")
ENABLE_DB = os.environ.get("ENABLE_DB", "true").lower() in ("1", "true", "yes")

use_local = not BUCKET_TRUSTED

# create s3 client only when using remote buckets
# Use the pattern from the example: prefer region_name from AWS_DEFAULT_REGION
if not use_local:
    region = os.environ.get("AWS_DEFAULT_REGION") or None
    # ignore S3_ENDPOINT and let boto3 resolve the endpoint via region/credentials
    s3 = boto3.client("s3", region_name=region)

# DB connection config from env
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "3306")),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "root"),
    "database": os.environ.get("DB_NAME", "cryptosight"),
}


def listar_chaves_trusted(prefix="medicoes/"):
    chaves = []
    if use_local:
        base = os.path.join(LOCAL_BUCKET_DIR, prefix)
        if not os.path.exists(base):
            return chaves
        for root, _, files in os.walk(base):
            for f in files:
                full = os.path.join(root, f)
                chave = os.path.relpath(full, start=LOCAL_BUCKET_DIR).replace(os.path.sep, "/")
                chaves.append(chave)
        return chaves

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_TRUSTED, Prefix=prefix):
        for obj in page.get("Contents", []):
            chaves.append(obj["Key"])
    return chaves


def ler_json(bucket, chave):
    if use_local:
        caminho = os.path.join(LOCAL_BUCKET_DIR, chave)
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    resp = s3.get_object(Bucket=bucket, Key=chave)
    return json.loads(resp["Body"].read().decode("utf-8"))


def gravar_json(bucket, chave, dados):
    if use_local:
        caminho = os.path.join(LOCAL_BUCKET_DIR, chave)
        pasta = os.path.dirname(caminho)
        os.makedirs(pasta, exist_ok=True)
        # idempotence: do not overwrite if file already exists
        if os.path.exists(caminho):
            print("skip gravar local: already exists:", chave)
            return
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False)
        return

    # idempotence: avoid re-writing an object that already exists in curated
    try:
        s3.head_object(Bucket=bucket, Key=chave)
        print("skip gravar s3: object already exists:", chave)
        return
    except Exception:
        # head_object will raise if not found; continue to put
        pass

    # write to a temporary file and use upload_file (follows your example pattern)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tf:
            tmp = tf.name
            json.dump(dados, tf, ensure_ascii=False)
        # upload_file uses the file on disk and the client's credentials/region
        s3.upload_file(tmp, bucket, chave)
    finally:
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def normalizar_registro(registro):
    # Normaliza o nome do timestamp e garante inteiros nas latências
    if "timestamp-medicao" in registro:
        ts = registro.pop("timestamp-medicao")
        registro["timestamp_medicao"] = ts
    elif "timestamp_medicao" not in registro:
        registro["timestamp_medicao"] = datetime.utcnow().isoformat() + "Z"

    # normalize timestamp to a consistent ISO format ending with Z
    ts_val = registro.get("timestamp_medicao")
    if isinstance(ts_val, str):
        try:
            # support strings like 2026-11-18T23:30:00Z or without Z
            t = ts_val.rstrip("Z")
            parsed = datetime.fromisoformat(t)
            registro["timestamp_medicao"] = parsed.replace(microsecond=0).isoformat() + "Z"
        except Exception:
            # fallback to now in a stable ISO form
            registro["timestamp_medicao"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    for campo in ["latencia_pool", "latencia_dns", "latencia_gateway"]:
        v = registro.get(campo)
        if isinstance(v, str):
            try:
                registro[campo] = int(v.replace("ms", "").strip())
            except Exception:
                registro[campo] = None
    return registro


def inserir_no_db(registro):
    # DB insert is optional. Skip unless explicitly enabled via ENABLE_DB env var.
    if not ENABLE_DB:
        print("DB insert disabled (ENABLE_DB not true). Skipping DB write for id_farm=", registro.get("id_farm"))
        return

    # Tenta inserir na tabela medicao; se falhar (tabela inexistente, etc.) apenas loga
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        print("falha conectar ao DB:", e)
        return

    try:
        cur = conn.cursor()
        sql = (
            "INSERT INTO medicao_rede (id_farm, latencia_pool, latencia_dns, latencia_gateway, timestamp_medicao)"
            " VALUES (%s, %s, %s, %s, %s)"
        )

        # try to convert timestamp_medicao to a python datetime so the connector writes a proper DATETIME/TIMESTAMP
        ts = registro.get("timestamp_medicao")
        if isinstance(ts, str):
            try:
                ts_clean = ts.rstrip("Z")
                ts_dt = datetime.fromisoformat(ts_clean)
            except Exception:
                ts_dt = None
        elif isinstance(ts, datetime):
            ts_dt = ts
        else:
            ts_dt = None

        vals = (
            registro.get("id_farm"),
            registro.get("latencia_pool"),
            registro.get("latencia_dns"),
            registro.get("latencia_gateway"),
            ts_dt,
        )
        cur.execute(sql, vals)
        conn.commit()
        print("inserido no DB id_farm=", registro.get("id_farm"))
    except mysql.connector.Error as e:
        # se a tabela não existir ou coluna inválida, logar e continuar
        print("erro ao inserir no DB:", e)
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    if not BUCKET_CURATED:
        raise RuntimeError("BUCKET_CURATED / BUCKET_GOLD não configurado. Defina a variável de ambiente.")

    chaves = listar_chaves_trusted("medicoes/")
    print(f"{len(chaves)} arquivos no trusted")

    for chave in chaves:
        try:
            registro = ler_json(BUCKET_TRUSTED or LOCAL_BUCKET_DIR, chave)
        except Exception as e:
            print("falha lendo trusted:", chave, e)
            continue

        registro = normalizar_registro(registro)

        try:
            gravar_json(BUCKET_CURATED, chave, registro)
            print("promovido p/ curated:", chave)
        except Exception as e:
            print("falha gravando curated:", chave, e)

        # Tenta inserir no banco (não obrigatório)
        inserir_no_db(registro)
