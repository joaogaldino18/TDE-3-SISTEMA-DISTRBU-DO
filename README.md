# TDE 03 – Detecção de Fraude com Apache Kafka

Cenário de detecção de fraude em tempo real usando **Apache Kafka**, **Python** e **SQLite**.

```
┌────────────┐   tópico "transacoes"   ┌──────────────┐      ┌──────────────┐
│  Producer  │ ──────────────────────▶ │   Consumer   │ ───▶ │  SQLite (db) │
│ (1 tx / 3s)│        Kafka            │ regras fraude│      └──────────────┘
└────────────┘                         └──────┬───────┘
                                              ▼
                                     alertas no console
```

## Estrutura

| Arquivo | Função |
|---|---|
| `docker-compose.yml` | Sobe o broker Kafka (modo KRaft, sem Zookeeper) |
| `config.py` | Configurações (broker, tópico, limites das regras) |
| `producer.py` | Gera e publica uma transação aleatória a cada 3 segundos |
| `consumer.py` | Lê as transações, aplica as regras, salva no banco e exibe alertas |
| `fraude.py` | Regras de detecção de fraude |
| `database.py` | Criação da tabela e persistência no SQLite (`transacoes.db`) |
| `consultar_banco.py` | Mostra um resumo das transações/fraudes salvas |
| `test_fraude.py` | Testes unitários das regras |

## Padrão da transação

```json
{
  "transaction_id": "3f1c2a8e-7b1d-4e0a-9c55-1a2b3c4d5e6f",
  "client_id": "C007",
  "amount": 12500.75,
  "city": "Curitiba",
  "type": "PIX",
  "timestamp": "2025-10-21T19:30:00.123456+00:00"
}
```

## Regras de fraude

| Código | Regra |
|---|---|
| `ALTO_VALOR` | Transação com `amount >= 10000` |
| `TEMPO_60s` | Mesmo cliente com 4 transações em menos de 60 segundos |
| `GEO_10m` | Mesmo cliente com 2 transações em cidades diferentes em até 10 minutos |

As regras de tempo e geolocalização usam o histórico do cliente salvo no banco, com base no `timestamp` da transação. Uma transação pode receber mais de um alerta.

## Como executar

Pré-requisitos: Docker e Python 3.10+.

```bash
# 1. Subir o Kafka
docker compose up -d

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Em um terminal, iniciar o consumidor
python consumer.py

# 4. Em outro terminal, iniciar o produtor
python producer.py

# (opcional) Consultar o banco
python consultar_banco.py

# (opcional) Rodar os testes das regras
python -m unittest -v test_fraude
```

Exemplo de saída do consumidor:

```
[TRANSAÇÃO OK] ID: d8d34809-911e-4785-ac67-26b76c75bb24
    Cliente: C003 | Valor: R$ 9857.43 | Cidade: Recife | Tipo: PIX | Data: 2025-10-21T17:16:04+00:00

[ALERTA DE FRAUDE] ALTO_VALOR, GEO_10m
    ID: c138b358-13a7-4033-b983-092b67835b4b
    Cliente: C003 | Valor: R$ 13199.10 | Cidade: Curitiba | Tipo: SAQUE | Data: 2025-10-21T17:18:04+00:00
```

## Banco de dados

Todas as transações (normais e fraudulentas) são salvas na tabela `transacoes` do arquivo `transacoes.db`:

| Coluna | Descrição |
|---|---|
| `transaction_id` | Identificador único |
| `client_id`, `amount`, `city`, `type`, `timestamp` | Dados da transação |
| `ts_epoch` | Timestamp em segundos (usado nas janelas de tempo) |
| `fraude` | `1` se houve alerta, `0` caso contrário |
| `motivos` | Códigos das regras disparadas (ex.: `TEMPO_60s,GEO_10m`) |
