"""
Orquestrador simples para executar a pipeline localmente:
 1) roda o gerador (network_collector) por N eventos
 2) executa ETL Raw -> Trusted (etl_raw_to_trusted.py)
 3) executa ETL Trusted -> Curated (etl_trusted_to_curated.py)

Uso:
  - Ajuste as variáveis de ambiente necessárias (buckets, credenciais AWS, etc.)
  - Rode: python3 run_pipeline.py [--events N]

O orquestrador usa subprocess para executar cada etapa como um script separado.
"""

import os
import sys
import subprocess

DEFAULT_EVENTS = 5


def run_step(cmd, env=None):
    print(f"-> Executando: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, env=env)
    except subprocess.CalledProcessError as e:
        print(f"Step falhou: {e}")
        raise


def main():
    # allow user to pass number of events as first arg or via ENV NUM_EVENTS
    args = sys.argv[1:]
    events = None
    # accept flags: --events N  and --enable-db / -d
    enable_db = False
    if args:
        i = 0
        while i < len(args):
            a = args[i]
            if a in ("--events", "-n"):
                i += 1
                if i >= len(args):
                    print("uso: run_pipeline.py [--events N] [--enable-db]")
                    sys.exit(2)
                try:
                    events = int(args[i])
                except Exception:
                    print("uso: run_pipeline.py [--events N] [--enable-db]")
                    sys.exit(2)
            elif a in ("--enable-db", "-d"):
                enable_db = True
            else:
                print(f"argumento desconhecido: {a}")
                sys.exit(2)
            i += 1
    else:
        events = int(os.getenv("NUM_EVENTS", str(DEFAULT_EVENTS)))

    # copy current environment but explicitly remove S3_ENDPOINT so subprocesses
    # don't accidentally try to use a local endpoint (localhost:9000) unless the
    # user intentionally sets it in the .env or shell that calls this script.
    env = os.environ.copy()
    env["NUM_EVENTS"] = str(events)
    # enable DB in subprocesses if requested
    if enable_db:
        env["ENABLE_DB"] = "true"
    # remove S3_ENDPOINT to avoid accidental local endpoints
    env.pop("S3_ENDPOINT", None)
    env.pop("LOCALSTACK_HOSTNAME", None)

    # 1) generator: network_collector.py
    run_step([sys.executable, "network_collector.py"], env=env)

    # 2) ETL raw -> trusted
    run_step([sys.executable, "etl_raw_to_trusted.py"], env=env)

    # 3) ETL trusted -> curated
    run_step([sys.executable, "etl_trusted_to_curated.py"], env=env)

    print("Pipeline finalizada com sucesso.")


if __name__ == "__main__":
    main()
