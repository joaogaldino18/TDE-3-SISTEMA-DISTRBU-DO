"""Produtor: publica uma transação aleatória no Kafka a cada 3 segundos."""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import INTERVALO_SEGUNDOS, KAFKA_BOOTSTRAP, TOPICO

CIDADES = ["São Paulo", "Rio de Janeiro", "Curitiba", "Belo Horizonte", "Porto Alegre", "Recife"]

# Poucos clientes para que a regra TEMPO_60s tenha chance de disparar.
# Cada cliente tem uma cidade "de casa"; às vezes transaciona em outra (GEO_10m).
CLIENTES = {f"C{i:03d}": CIDADES[i % len(CIDADES)] for i in range(1, 21)}
CHANCE_OUTRA_CIDADE = 0.03
TIPOS = ["COMPRA", "SAQUE", "PIX", "TRANSFERENCIA"]


def gerar_transacao():
    """Gera uma transação no padrão do projeto."""
    # ~10% das transações com valor alto para exercitar a regra ALTO_VALOR
    if random.random() < 0.10:
        amount = round(random.uniform(10000, 50000), 2)
    else:
        amount = round(random.uniform(5, 9999.99), 2)

    client_id = random.choice(list(CLIENTES))
    if random.random() < CHANCE_OUTRA_CIDADE:
        city = random.choice([c for c in CIDADES if c != CLIENTES[client_id]])
    else:
        city = CLIENTES[client_id]

    return {
        "transaction_id": str(uuid.uuid4()),
        "client_id": client_id,
        "amount": amount,
        "city": city,
        "type": random.choice(TIPOS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def relatorio_entrega(err, msg):
    if err is not None:
        print(f"[ERRO] Falha ao enviar: {err}")


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"Produtor iniciado. Publicando no tópico '{TOPICO}' a cada {INTERVALO_SEGUNDOS}s (Ctrl+C para sair)\n")

    try:
        while True:
            transacao = gerar_transacao()
            producer.produce(
                TOPICO,
                key=transacao["client_id"],
                value=json.dumps(transacao, ensure_ascii=False).encode("utf-8"),
                callback=relatorio_entrega,
            )
            producer.poll(0)
            print(
                f"[ENVIADA] {transacao['client_id']} | R$ {transacao['amount']:>10.2f} | "
                f"{transacao['city']:<15} | {transacao['type']:<13} | {transacao['transaction_id']}"
            )
            time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("\nEncerrando produtor...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
