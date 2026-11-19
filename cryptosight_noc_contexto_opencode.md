# CryptoSight NOC — Contexto de planejamento para OpenCode

> **Objetivo deste documento:** servir como janela de contexto para o GPT-5 Mini no OpenCode. Use-o como referência do projeto antes de propor alterações no código. O trabalho esperado é evoluir o coletor e o pipeline de dados de forma incremental, mantendo o que já funciona e explicando as decisões técnicas.

---

## 1. Visão geral do projeto

O CryptoSight possui uma tela de **Dashboard NOC** destinada a um analista de Network Operations Center (NOC), responsável por acompanhar a saúde operacional de farms de mineração de criptomoedas.

A tela atual apresenta:

- **Shares aceitos:** quantidade de shares aceitos pelo pool.
- **Shares rejeitados:** percentual de shares rejeitados, com limite de referência de 2%.
- **Shares stale:** percentual de shares que chegaram atrasados, também com limite de referência de 2%.
- **Latência do pool:** latência em milissegundos, com limite de referência de 150 ms.
- **Correlação de índices:** séries temporais de hashrate efetivo, latência até o pool e shares rejeitados + stale.
- **Diagnóstico do CryptoAgent:** estado da rede local, caminho até o pool e hardware.
- **Linha temporal:** caminho entre a farm, gateway, DNS, rota externa e pool.

O objetivo é alimentar essa tela com dados que representem, de maneira plausível e útil, a operação de uma farm. Os dados devem permitir identificar incidentes, acompanhar tendências e apoiar o diagnóstico de problemas.

**Importante:** o sistema está em fase de desenvolvimento acadêmico/prototipação. Parte das métricas será coletada de uma máquina real e parte será simulada. A origem de cada métrica deve ficar explícita nos dados para que ninguém confunda simulação com telemetria real.

---

## 2. Objetivo técnico

Construir um fluxo contínuo de geração/coleta, armazenamento e tratamento de dados:

1. Identificar as farms cadastradas no MySQL.
2. Coletar métricas reais da máquina/agente sempre que houver uma fonte local confiável.
3. Simular métricas que dependem de um minerador, pool ou infraestrutura que não esteja disponível no ambiente de teste.
4. Gerar também registros propositalmente sujos, em uma proporção configurável.
5. Enviar os dados brutos para o bucket Bronze (Raw) do S3.
6. Executar ETLs em Python para validar, normalizar, enriquecer e agregar os dados.
7. Armazenar os resultados nas camadas Trusted e Curated.
8. Permitir que a API do CryptoSight consuma os dados tratados e que um analista faça análises históricas.

O coletor deve funcionar continuamente, com intervalo configurável, e suportar múltiplas farms.

---

## 3. Armazenamento S3 — arquitetura desejada

Os três buckets já existentes são:

| Camada | Bucket | Finalidade |
|---|---|---|
| Bronze / Raw | `cryptosight-s3-dev-raw` | Guardar os eventos originais, inclusive registros sujos, sem sobrescrever ou corrigir os dados de origem. |
| Trusted | `cryptosight-s3-dev-trusted` | Guardar registros validados, normalizados e com qualidade conhecida. |
| Curated | `cryptosight-s3-dev-curated` | Guardar dados prontos para consumo analítico/API, como agregações, indicadores e séries temporais. |

Os buckets são separados. Não tratar `Bronze`, `Raw`, `Trusted`, `Silver` e `Curated` como se fossem nomes intercambiáveis sem definir um padrão. Neste projeto, a nomenclatura oficial desejada é **Bronze/Raw → Trusted → Curated**.

Links do console AWS:
- Bronze/Raw: https://us-east-1.console.aws.amazon.com/s3/buckets/cryptosight-s3-dev-raw?region=us-east-1
- Trusted: https://us-east-1.console.aws.amazon.com/s3/buckets/cryptosight-s3-dev-trusted?region=us-east-1
- Curated: https://us-east-1.console.aws.amazon.com/s3/buckets/cryptosight-s3-dev-curated?region=us-east-1

### Organização sugerida dos objetos

Usar prefixos particionados por data e, quando fizer sentido, por farm e tipo de dado. Exemplo:

```text
network/
  year=2026/
    month=11/
      day=18/
        farm_id=25/
          <timestamp>-<uuid>.json
```

A estrutura exata pode ser refinada durante o planejamento. Evitar criar milhares de arquivos minúsculos sem avaliar o custo/benefício. Para o protótipo, um JSON por evento é aceitável; futuramente pode ser interessante agrupar eventos em JSONL ou Parquet.

### Regras das camadas

**Bronze**
- Preservar o evento recebido, sem limpeza destrutiva.
- Manter os valores inválidos e os campos ausentes/nulos que foram gerados intencionalmente.
- Incluir metadados de ingestão, como identificador do evento, horário de coleta, horário de ingestão, versão do schema e origem.
- Não sobrescrever eventos já gravados.

**Trusted**
- Validar tipos, campos obrigatórios, unidades, intervalos e timestamps.
- Normalizar valores que possam ser recuperados com segurança (por exemplo, `"212ms"` para `212`).
- Separar registros corrigíveis de registros inválidos que devem ser rejeitados ou colocados em uma área de quarentena.
- Registrar o motivo de rejeição/correção. Não descartar silenciosamente dados ruins.
- Tornar o processamento idempotente, para que executar o ETL novamente não gere duplicações.

**Curated**
- Produzir estruturas adequadas à API e à análise NOC.
- Calcular indicadores por farm e janela de tempo (por exemplo, médias, máximos, percentis, taxas e contagens).
- Preservar a possibilidade de rastrear um indicador até os eventos de origem.
- Definir claramente a granularidade de cada conjunto: evento, minuto, cinco minutos, hora etc.

---

## 4. Métricas que o domínio precisa representar

Não limitar o projeto às três latências do protótipo atual. O conjunto de dados deve representar os principais aspectos operacionais de uma farm.

### 4.1 Rede

Métricas desejadas:

| Métrica | Unidade | Fonte pretendida | Observações |
|---|---|---|---|
| Latência do gateway | ms | Coleta real, quando possível | Teste até o gateway configurado. |
| Latência DNS | ms | Coleta real | Medir resolução de um domínio configurado; registrar qual servidor/nome foi consultado. |
| Latência do pool | ms | Coleta real, se o endpoint permitir | Medir conexão/handshake ou resposta do endpoint do pool. Não confundir latência TCP com latência de resposta da aplicação. |
| Latência da rota externa | ms | Coleta real, quando possível | Pode envolver ping/traceroute; a rota pode não responder a ICMP. |
| Perda de pacotes | % | Coleta real, quando possível | Informar quantidade enviada/recebida e método utilizado. |
| Jitter | ms | Derivada de medições reais | Variação da latência ao longo de uma janela. |
| Bytes enviados/recebidos | bytes ou bytes/s | `psutil` | Usar contadores da interface de rede e calcular taxas por diferença entre coletas. |
| Pacotes enviados/recebidos | pacotes | `psutil` | Contadores acumulados da interface. |
| Erros e drops de rede | contagem | `psutil` | Usar `net_io_counters`; documentar limitações do sistema operacional/interface. |
| Interface ativa | boolean/string | `psutil` | Estado e identificação da interface, quando disponíveis. |

**Limite importante do `psutil`:** ele fornece contadores e informações das interfaces, mas não mede sozinho a latência até DNS, gateway ou pool. Essas medições exigem testes ativos de rede (por exemplo, sockets, resolução DNS, ping ou requisições TCP/HTTP). Algumas redes bloqueiam ICMP, portanto a ausência de resposta ao ping não prova necessariamente que o serviço está indisponível.

### 4.2 Mineração / shares

Métricas desejadas:

| Métrica | Unidade | Fonte pretendida | Observações |
|---|---|---|---|
| Shares aceitos | contagem | API/log do minerador ou simulação | Contador acumulado ou incremento por intervalo; definir qual será armazenado. |
| Shares rejeitados | contagem e % | API/log do minerador ou simulação | Manter numerador e denominador para calcular o percentual corretamente. |
| Shares stale | contagem e % | API/log do minerador ou simulação | Shares aceitos/rejeitados por atraso, conforme a classificação fornecida pelo minerador/pool. |
| Hashrate efetivo | MH/s, GH/s etc. | API/log do minerador ou simulação | Registrar unidade explicitamente. |
| Hashrate reportado | unidade de hashrate | API/log do minerador ou simulação | Pode divergir do efetivo. Definir a semântica antes de implementar. |
| Estado do minerador | enum/string | Processo/API/log ou simulação | Ex.: online, offline, reconectando, erro. |
| Uptime do minerador | segundos | Processo/API ou simulação | Não confundir com uptime do sistema operacional. |
| Temperatura da GPU | °C | Ferramenta/SDK do fabricante ou simulação | `psutil` não oferece telemetria universal de temperatura de GPU. |
| Potência da GPU | W | Ferramenta/SDK do fabricante ou simulação | Depende do fabricante e driver. |
| Uso da GPU | % | Ferramenta/SDK do fabricante ou simulação | Não assumir que `psutil` mede GPU. |

**Limite importante:** `psutil` não sabe quantos shares foram aceitos, rejeitados ou stale pelo pool. Esses números devem vir de uma API/log do minerador/pool quando houver integração real; caso contrário, devem ser simulados e marcados como simulados.

### 4.3 Hardware e sistema operacional

Métricas que podem ser coletadas com `psutil`:

- CPU: percentual de uso, quantidade de núcleos lógicos e, se aplicável, frequência.
- Memória: total, disponível, usada e percentual.
- Disco: espaço total, usado, disponível e percentual por partição.
- I/O de disco: bytes e operações de leitura/escrita, quando suportados.
- Rede: bytes, pacotes, erros e drops por interface.
- Processos: existência/estado de processos relevantes, se o agente tiver permissão.
- Uptime do sistema operacional.

Temperatura, potência e telemetria específica de GPU dependem de bibliotecas/SDKs do fabricante (por exemplo, NVML para GPUs NVIDIA) ou devem ser simuladas. Não atribuir ao `psutil` capacidades que ele não possui.

### 4.4 Diagnóstico NOC

A tela também precisa suportar informações que ajudem o analista a localizar a origem do incidente:

- Estado da rede local.
- Estado do caminho até o pool.
- Estado do hardware.
- Gateway e DNS medidos.
- Rota externa medida ou explicitamente não medida.
- Pool medido e endpoint monitorado.
- Horário do início do problema e evolução dos indicadores.
- Severidade/estado do diagnóstico, com regras transparentes.

O diagnóstico pode ser calculado posteriormente na camada Curated ou pela API. Evitar misturar a coleta de métricas com regras de diagnóstico no mesmo módulo.

---

## 5. Separação entre dados reais e simulados

O projeto precisa permitir uma transição gradual da simulação para a coleta real.

Cada métrica deve carregar informação suficiente para identificar sua origem. Uma abordagem possível:

```json
{
  "metric": "latencia_pool",
  "value": 212,
  "unit": "ms",
  "source_type": "simulated",
  "collector": "network_collector",
  "measured_at": "2026-11-18T23:30:00Z"
}
```

O formato acima é apenas uma referência conceitual. Não é obrigatório adotar uma estrutura aninhada; o importante é não perder a procedência de cada valor.

Valores possíveis para `source_type`:
- `real`: medido no ambiente.
- `simulated`: produzido pelo simulador.
- `derived`: calculado a partir de outras medições.

Não marcar um registro inteiro como real se apenas parte de suas métricas veio de sensores reais. A procedência deve ser definida por métrica ou por grupo de métricas.

---

## 6. Dados artificialmente sujos

Uma parte dos registros enviados ao Bronze deve conter problemas intencionais para exercitar o ETL. A proporção deve ser configurável por variável de ambiente (`DIRTY_RATE`, por exemplo).

Tipos de sujeira já considerados:

- Valor nulo em campo que normalmente deveria ser numérico.
- Número serializado como texto com unidade, por exemplo `"212ms"`.
- Outlier impossível ou absurdo, por exemplo `-5` ou `99999`.
- Campo ausente.
- Tipo incorreto (string no lugar de número, lista/objeto no lugar de escalar).
- Timestamp inválido ou ausente.
- Unidade inconsistente (por exemplo, valor em segundos enviado como se fosse milissegundos).
- Identificador de farm inexistente, apenas se isso for útil para testar integridade referencial.
- Duplicidade de evento, para testar idempotência.

Regras:
- A sujeira deve ser intencional, controlada e reproduzível quando possível (usar seed configurável para testes).
- Não corromper todos os campos do mesmo registro de forma indiscriminada.
- Registrar quais anomalias foram introduzidas, preferencialmente em metadados de teste, sem “consertar” o evento Bronze.
- Separar dados sujos de falhas reais de coleta. Uma falha de coleta não deve ser mascarada como sujeira artificial.
- Evitar valores aleatórios sem relação com o domínio. Os valores limpos simulados devem ter faixas e comportamentos plausíveis.

---

## 7. Dados e formato de evento

O coletor atual gera um registro simples:

```python
agora = datetime.now(timezone.utc)

return {
    "id_farm": id_farm,
    "latencia_pool": random.randint(100, 400),
    "latencia_dns": random.randint(5, 60),
    "latencia_gateway": random.randint(5, 50),
    "timestamp-medicao": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
}
```

Esse é o ponto de partida, não o schema final.

Pontos a definir:
- Padronizar o nome do timestamp. Preferir um identificador consistente, como `measured_at` ou `timestamp_medicao`, em vez de misturar hífen e underscore.
- Diferenciar horário da medição (`measured_at`) e horário de ingestão (`ingested_at`).
- Incluir `event_id` único.
- Incluir `id_farm`.
- Incluir versão do schema (`schema_version`).
- Definir unidades explicitamente.
- Definir se cada evento contém todas as métricas ou se haverá eventos separados por domínio (rede, mineração, hardware).
- Definir a frequência de coleta por grupo de métricas.
- Definir como representar valor indisponível: `null`, campo ausente ou status de coleta. Não usar essas opções indistintamente.

Exemplo de envelope possível (ilustrativo):

```json
{
  "event_id": "uuid",
  "schema_version": "1.0",
  "id_farm": 25,
  "measured_at": "2026-11-18T23:30:00Z",
  "ingested_at": "2026-11-18T23:30:01Z",
  "collector": "cryptosight-agent",
  "metrics": {
    "latencia_pool_ms": 212,
    "latencia_dns_ms": 18,
    "latencia_gateway_ms": 2,
    "shares_accepted": 1284,
    "shares_rejected_pct": 5.8,
    "shares_stale_pct": 1.6
  },
  "provenance": {
    "latencia_pool_ms": "simulated",
    "latencia_dns_ms": "real",
    "latencia_gateway_ms": "real",
    "shares_accepted": "simulated"
  }
}
```

Não implementar esse exemplo sem antes avaliar se o formato atende à API, ao ETL e ao volume esperado.

---

## 8. Estado atual do código

### 8.1 `network_collector.py`

O arquivo atual:
- Lê configurações de conexão MySQL e S3 por variáveis de ambiente.
- Busca os IDs da tabela `farm` uma vez no início.
- Gera latências aleatórias para pool, DNS e gateway.
- Aplica sujeira em um campo de latência com probabilidade `DIRTY_RATE`.
- Grava um JSON por evento no bucket Raw.
- Repete o processo em loop com intervalo configurável.

Trechos e configurações atuais:

```python
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

BUCKET_RAW = os.getenv("BUCKET_RAW")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

DIRTY_RATE = float(os.getenv("DIRTY_RATE", "0.2"))
INTERVALO = float(os.getenv("INTERVALO_SEGUNDOS", "1"))

CAMPOS_LATENCIA = [
    "latencia_pool",
    "latencia_dns",
    "latencia_gateway"
]
```

Busca de farms:

```python
def buscar_ids_farm():
    conexao = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    try:
        with conexao.cursor() as cursor:
            cursor.execute("SELECT id FROM farm")
            linhas = cursor.fetchall()
    finally:
        conexao.close()

    return [linha[0] for linha in linhas]
```

Geração e sujeira:

```python
def gerar_registro_limpo(id_farm):
    agora = datetime.now(timezone.utc)
    return {
        "id_farm": id_farm,
        "latencia_pool": random.randint(100, 400),
        "latencia_dns": random.randint(5, 60),
        "latencia_gateway": random.randint(5, 50),
        "timestamp-medicao": agora.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def sujar_registro(registro):
    campo = random.choice(CAMPOS_LATENCIA)
    tipo = random.choice(["nulo", "texto", "outlier"])

    if tipo == "nulo":
        registro[campo] = None
    elif tipo == "texto":
        registro[campo] = f"{registro[campo]}ms"
    else:
        registro[campo] = random.choice([-5, 99999])

    return registro
```

Gravação:

```python
def salvar_no_bucket(s3, registro):
    agora = datetime.now(timezone.utc)
    chave = (
        f"medicoes/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/"
        f"{agora:%H%M%S}_{uuid.uuid4().hex[:8]}.json"
    )
    s3.put_object(
        Bucket=BUCKET_RAW,
        Key=chave,
        Body=json.dumps(registro).encode("utf-8"),
        ContentType="application/json",
    )
    return chave
```

Loop principal:
- Valida algumas variáveis obrigatórias.
- Busca farms uma vez.
- Cria um cliente boto3.
- Escolhe uma farm aleatória a cada iteração.
- Gera, possivelmente suja e grava um evento.
- Aguarda `INTERVALO`.
- Captura `KeyboardInterrupt`.

### 8.2 `configenv.py`

O módulo atual:
- Carrega `.env` com `python-dotenv`.
- Lê os nomes dos buckets via `AWS_BUCKET_RAW`, `AWS_BUCKET_TRUSTED` e `AWS_BUCKET_CURATED`.
- Cria um cliente S3 via `boto3.Session()`.
- Monta parâmetros MySQL e abre uma conexão/cursor no momento da importação.

Ponto de atenção: abrir conexão com o banco no momento do import dificulta testes, tratamento de falhas e reutilização do módulo. Preferir funções que criem conexões quando necessário, com fechamento explícito.

### 8.3 `etl_silver.py`

O ETL atual:
- Lê `BUCKET_BRONZE`, `BUCKET_SILVER` e `S3_ENDPOINT` do ambiente.
- Lista objetos do prefixo `medicoes/` no Bronze.
- Baixa cada JSON.
- Tenta converter strings como `"212ms"` em inteiros.
- Descarta o registro se algum campo de latência estiver nulo, negativo ou acima de 5000.
- Grava o registro limpo no bucket Silver com a mesma chave.

Pontos de atenção:
- O código usa `Silver`, mas a arquitetura desejada utiliza `Trusted`.
- O ETL ainda não promove dados para Curated.
- A limpeza não trata de forma robusta texto inválido, campos ausentes, timestamp incorreto ou outros tipos inesperados.
- Um registro descartado não deve desaparecer sem registro do motivo.
- Reprocessar o prefixo inteiro pode duplicar trabalho; planejar checkpoint/controle de objetos processados ou idempotência.
- A listagem deve lidar com paginação (o código atual já usa paginator).
- Definir como lidar com objetos que já existem no destino.

### 8.4 `lib.txt`

Dependências listadas atualmente:

```text
psutil
getmac
pandas
mysql-connector-python
python-dotenv
boto3
```

Avaliar novas dependências somente quando houver necessidade concreta. Não adicionar bibliotecas por antecipação.

---

## 9. Inconsistências e riscos conhecidos para revisar

1. **Nomes de variáveis de ambiente:** `network_collector.py` usa `BUCKET_RAW`; `configenv.py` usa `AWS_BUCKET_RAW`; `etl_silver.py` usa `BUCKET_BRONZE` e `BUCKET_SILVER`. Padronizar os nomes.
2. **Nomenclatura das camadas:** adotar `RAW`, `TRUSTED` e `CURATED` de ponta a ponta. Evitar alternar entre Bronze/Silver/Gold e Raw/Trusted/Curated sem uma convenção documentada.
3. **Porta do MySQL:** converter `DB_PORT` para inteiro antes de passar ao conector, se necessário.
4. **Lista de farms vazia:** validar o resultado da consulta antes de executar `random.choice(ids_farm)`.
5. **Conexão com MySQL:** tratar indisponibilidade, timeout, reconexão e fechamento adequado.
6. **Credenciais AWS:** preferir IAM Role/credential chain do boto3 em ambiente AWS. Em ambiente local, usar perfil AWS ou variáveis de ambiente; não versionar credenciais no repositório nem gravá-las nos eventos.
7. **Endpoint S3:** `endpoint_url` deve ser opcional; não passar configurações incompatíveis com o ambiente AWS real.
8. **Timestamp:** padronizar o nome e manter UTC, com formato ISO 8601.
9. **Qualidade dos dados:** distinguir falha de medição, dado indisponível e sujeira artificial.
10. **Semântica dos shares:** não gerar percentuais sem definir o total de shares e a janela de cálculo.
11. **Contadores versus taxas:** `psutil` retorna vários contadores acumulados; para bytes/s ou pacotes/s, calcular a diferença entre duas coletas e considerar reinicialização/overflow.
12. **Frequência:** coletar tudo a cada segundo pode ser desnecessário ou caro. Definir intervalos por tipo de métrica.
13. **Crescimento do S3:** um JSON por segundo por farm pode gerar muitos objetos. Avaliar lote/JSONL e política de retenção quando o protótipo estiver validado.
14. **Falhas de envio:** definir retry com backoff e evitar perder eventos silenciosamente.
15. **Observabilidade do próprio coletor:** registrar erros de conexão, coleta e upload sem inserir logs sensíveis.

---

## 10. Direção de arquitetura sugerida

Separar responsabilidades em módulos, sem fazer uma refatoração grande de uma só vez. Uma possível divisão:

```text
cryptosight-noc-data-etl/
├── collectors/
│   ├── network_collector.py
│   ├── system_collector.py
│   ├── mining_collector.py
│   └── simulated_collector.py
├── generators/
│   └── dirty_data.py
├── storage/
│   └── s3_storage.py
├── database/
│   └── farm_repository.py
├── etl/
│   ├── raw_to_trusted.py
│   └── trusted_to_curated.py
├── schemas/
│   └── ...
├── config/
│   └── settings.py
├── tests/
├── .env.example
├── requirements.txt
└── README.md
```

Essa estrutura é uma proposta, não uma exigência. O GPT deve primeiro analisar a estrutura real do repositório e adaptar a organização existente, evitando mover arquivos sem necessidade.

### Responsabilidades sugeridas

- **Collectors:** obter métricas reais de rede e sistema.
- **Simulated collector:** produzir métricas que ainda não têm integração real.
- **Dirty data generator:** introduzir anomalias de teste de forma controlada.
- **Storage:** serializar e gravar eventos no S3.
- **Repository:** buscar farms e configurações no MySQL.
- **ETL Raw → Trusted:** validar e normalizar.
- **ETL Trusted → Curated:** agregar e preparar dados para consumo.
- **API:** consumir a camada Curated ou uma persistência derivada dela; a decisão deve considerar latência e frequência de atualização.

---

## 11. Plano de evolução esperado

Trabalhar em etapas pequenas, validando cada uma antes de avançar.

### Etapa 1 — Diagnóstico do estado atual
- Inspecionar os arquivos reais do repositório.
- Identificar o que já funciona e o que está incompleto.
- Conferir `.env.example`, dependências, Dockerfile e forma de execução.
- Não substituir código funcional sem justificar.

### Etapa 2 — Contrato/schema dos eventos
- Definir métricas, tipos, unidades, nomes e timestamps.
- Definir procedência real/simulada/derivada.
- Definir campos obrigatórios e opcionais.
- Definir versão do schema.
- Criar exemplos de eventos limpos e sujos.

### Etapa 3 — Coleta real com `psutil`
- Implementar coleta de CPU, memória, disco, uptime e contadores de rede.
- Calcular taxas a partir de diferenças entre coletas.
- Identificar interfaces e partições.
- Tratar ausência de permissões ou métricas não suportadas.
- Não simular silenciosamente um dado cuja coleta real falhou.

### Etapa 4 — Testes ativos de rede
- Medir gateway, DNS e endpoint do pool usando métodos adequados.
- Definir timeout, quantidade de tentativas e tratamento de falhas.
- Medir perda de pacotes/jitter quando tecnicamente possível.
- Registrar o método e o destino de cada teste.
- Não considerar ICMP como único método de verificação.

### Etapa 5 — Simulação de mineração
- Simular shares aceitos, rejeitados e stale com relações matemáticas coerentes.
- Simular hashrate, estado do minerador e incidentes plausíveis.
- Evitar valores independentes sem coerência temporal.
- Preparar uma interface para futura integração com API/log de minerador ou pool.
- Identificar claramente tudo que for simulado.

### Etapa 6 — Geração de sujeira
- Configurar taxa de sujeira e tipos de anomalia.
- Manter registros limpos como base e aplicar mutações controladas.
- Testar nulos, tipos inválidos, outliers, campos ausentes, timestamps e duplicidades.
- Garantir que o Bronze preserve o evento original.

### Etapa 7 — Ingestão Bronze
- Padronizar configuração e credenciais.
- Gravar eventos com `event_id` e chave única.
- Adicionar retries e logs de falha.
- Validar upload e estrutura de prefixos.
- Garantir que a execução contínua não dependa de uma única consulta inicial para sempre, caso farms possam ser cadastradas durante a execução.

### Etapa 8 — ETL Bronze → Trusted
- Validar schema e unidades.
- Normalizar formatos recuperáveis.
- Separar válidos, corrigidos e rejeitados/quarentena.
- Registrar motivos de rejeição e correção.
- Implementar idempotência e controle de processamento.

### Etapa 9 — ETL Trusted → Curated
- Definir agregações e janelas de tempo úteis ao NOC.
- Calcular percentuais de shares com numeradores/denominadores.
- Calcular médias, máximos, percentis e taxas de erro.
- Preparar dados para gráficos e cards da dashboard.
- Validar os resultados com exemplos conhecidos.

### Etapa 10 — Integração com API e dashboard
- Definir contrato de consumo da API.
- Definir se a API lerá diretamente o S3, uma tabela analítica ou uma base operacional.
- Garantir que os campos correspondam aos cards e gráficos da tela.
- Diferenciar ausência de dados de estado saudável.
- Documentar limites e origem dos indicadores.

---

## 12. Regras para a simulação ser útil ao NOC

A simulação não deve produzir apenas números aleatórios. Ela deve criar cenários coerentes e interpretáveis.

Exemplos de cenários:
- **Normal:** latência baixa e estável, poucas rejeições, hashrate estável.
- **Congestionamento de rota:** latência externa e do pool sobem; gateway e DNS podem permanecer normais.
- **Problema de DNS:** latência/erro DNS aumenta; conexão ao pool pode falhar ou ficar indisponível.
- **Problema no gateway/rede local:** latência do gateway, perda de pacotes e erros de interface aumentam.
- **Pool instável:** latência e falhas de conexão ao pool aumentam; shares rejeitados/stale podem crescer.
- **Hardware degradado:** temperatura/uso/erros de GPU ou CPU mudam e o hashrate cai.
- **Minerador desconectado:** hashrate efetivo zera ou fica indisponível; shares deixam de ser enviados.
- **Problema isolado de coleta:** a métrica fica indisponível, mas isso não deve ser interpretado automaticamente como falha do equipamento.

Os cenários devem ter duração, início e recuperação. Métricas correlacionadas devem mudar em conjunto de maneira plausível. Por exemplo, um problema de rota externa pode aumentar a latência do pool e a taxa de stale, sem necessariamente alterar a latência do gateway.

Os limites apresentados na dashboard (2% para rejeitados/stale e 150 ms para latência do pool) são referências visuais atuais. Não tratá-los como limites universais: futuramente devem ser configuráveis por farm/pool.

---

## 13. Critérios de qualidade e aceite

Uma etapa pode ser considerada concluída quando:

- O código executa no ambiente atual sem quebrar funcionalidades já existentes.
- A origem de cada métrica está identificada.
- Unidades e timestamps são consistentes.
- O coletor trata falhas de rede, banco e S3 sem encerrar inesperadamente.
- O Bronze mantém os dados originais, inclusive os sujos.
- O Trusted registra o que foi validado, corrigido e rejeitado.
- O Curated entrega indicadores reproduzíveis e rastreáveis.
- Reprocessar dados não cria duplicações indevidas.
- Existem testes para casos normais, anômalos e falhas.
- A documentação de execução e variáveis de ambiente está atualizada.

---

## 14. Como o GPT-5 Mini no OpenCode deve trabalhar

Atue como um **engenheiro de dados/engenheiro de software sênior**, ajudando a refinar um projeto existente. Não assuma que o código está do zero.

### Forma de trabalho obrigatória

1. Primeiro, inspecione a estrutura real do repositório e os arquivos relevantes.
2. Compare o código real com este documento e identifique divergências.
3. Apresente um plano curto para a próxima etapa antes de fazer mudanças amplas.
4. Trabalhe em pequenas entregas, com alterações localizadas.
5. Explique o motivo das mudanças e os trade-offs importantes.
6. Preserve o que já funciona.
7. Não invente nomes de tabelas, colunas, endpoints, variáveis ou serviços sem verificar o repositório.
8. Não implemente todas as etapas de uma vez.
9. Não adicione dependências sem explicar por que são necessárias.
10. Ao alterar código, informe os arquivos modificados e como testar.
11. Se faltar informação essencial, faça uma pergunta objetiva; não faça uma bateria de perguntas preliminares.
12. Diferencie claramente fatos encontrados no código, decisões propostas e pontos ainda em aberto.

### Prioridade inicial

Comece pela **Etapa 1 — Diagnóstico do estado atual** e depois proponha a ordem mais segura para definir o schema e evoluir o `network_collector.py`.

Não comece reescrevendo o projeto inteiro. A primeira entrega deve ser um diagnóstico do repositório, dos problemas concretos e de um plano incremental.

---

## 15. Decisões ainda em aberto

O planejamento deve ajudar a decidir, com justificativa:

- O schema será um evento consolidado por farm ou eventos separados por domínio?
- Qual será a frequência adequada para rede, hardware e mineração?
- Como identificar e configurar gateway, DNS, endpoint/porta do pool e interface de rede por farm?
- O agente será executado dentro de cada farm ou haverá um coletor central?
- Como obter métricas reais do minerador/pool no futuro?
- Qual formato será usado no S3: JSON individual, JSONL ou Parquet?
- Como será feito o controle de eventos já processados pelo ETL?
- A camada Curated será consumida diretamente do S3 ou materializada em banco para a API?
- Quais janelas de agregação a dashboard precisa (segundos, minutos, horas)?
- Como configurar limites de alerta por farm?
- Quais dados precisam ser persistidos para auditoria e investigação de incidentes?

Não é necessário responder tudo antes de começar. Resolver as decisões na ordem em que se tornarem necessárias.

---

## 16. Resumo executivo

O projeto parte de um coletor Python funcional em grande parte, que atualmente simula latências, introduz sujeira em alguns registros e grava JSON no S3. A evolução desejada é transformá-lo em uma ingestão de telemetria NOC para farms de mineração, combinando:

- **Dados reais:** métricas do sistema e contadores de rede via `psutil`, além de testes ativos de conectividade quando possíveis.
- **Dados simulados:** shares, hashrate e métricas que dependem de minerador/pool/GPU sem integração disponível.
- **Dados sujos controlados:** para testar a robustez dos ETLs.
- **S3 em três camadas:** Bronze/Raw, Trusted e Curated.
- **ETLs Python:** validação, normalização, quarentena, agregação e preparação para consumo.
- **Consumo NOC:** indicadores que ajudem a detectar, correlacionar e diagnosticar problemas de rede, mineração e hardware.

A prioridade é construir uma base de dados confiável, rastreável e coerente com o domínio — não apenas gerar números para preencher os gráficos.
