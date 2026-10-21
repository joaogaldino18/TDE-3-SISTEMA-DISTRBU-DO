"""Consumidor: lê as transações do Kafka, aplica as regras de fraude,
salva no banco e exibe alertas no console."""

import json
import os

from confluent_kafka import Consumer, KafkaError

from config import GRUPO_CONSUMIDOR, KAFKA_BOOTSTRAP, TOPICO
from database import conectar, salvar
from fraude import detectar

VERMELHO = "\033[91m"
VERDE = "\033[92m"
RESET = "\033[0m"


def exibir(transacao, motivos):
    detalhes = (
        f"ID: {transacao['transaction_id']}\n"
        f"    Cliente: {transacao['client_id']} | Valor: R$ {transacao['amount']:.2f} | "
        f"Cidade: {transacao['city']} | Tipo: {transacao['type']} | Data: {transacao['timestamp']}"
    )
    if motivos:
        print(f"{VERMELHO}[ALERTA DE FRAUDE] {', '.join(motivos)}{RESET}\n    {detalhes}\n")
    else:
        print(f"{VERDE}[TRANSAÇÃO OK]{RESET} {detalhes}\n")


def main():
    os.system("")  # habilita cores ANSI no console do Windows
    conn = conectar()
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GRUPO_CONSUMIDOR,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPICO])
    print(f"Consumidor iniciado. Lendo o tópico '{TOPICO}' (Ctrl+C para sair)\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERRO] {msg.error()}")
                continue

            transacao = json.loads(msg.value().decode("utf-8"))
            ts, motivos = detectar(conn, transacao)
            salvar(conn, transacao, ts, motivos)
            exibir(transacao, motivos)
    except KeyboardInterrupt:
        print("\nEncerrando consumidor...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
