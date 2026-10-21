"""Regras de detecção de fraude.

- ALTO_VALOR: amount >= 10000
- TEMPO_60s:  mesmo cliente com 4 transações em menos de 60s
- GEO_10m:    mesmo cliente com 2 transações em cidades diferentes em até 10 minutos
"""

from datetime import datetime

from config import (
    JANELA_GEO_SEGUNDOS,
    JANELA_TEMPO_SEGUNDOS,
    LIMITE_ALTO_VALOR,
    QTD_TRANSACOES_TEMPO,
)
from database import historico_cliente


def para_epoch(timestamp_iso):
    return datetime.fromisoformat(timestamp_iso).timestamp()


def detectar(conn, transacao):
    """Retorna (ts_epoch, lista de motivos) considerando o histórico salvo no banco.

    Deve ser chamada ANTES de salvar a transação atual.
    """
    motivos = []
    ts = para_epoch(transacao["timestamp"])
    cliente = transacao["client_id"]

    if transacao["amount"] >= LIMITE_ALTO_VALOR:
        motivos.append("ALTO_VALOR")

    # Anteriores dentro da janela de 60s + a atual
    recentes = [
        t for t in historico_cliente(conn, cliente, ts - JANELA_TEMPO_SEGUNDOS, ts)
        if t["ts_epoch"] > ts - JANELA_TEMPO_SEGUNDOS
    ]
    if len(recentes) + 1 >= QTD_TRANSACOES_TEMPO:
        motivos.append("TEMPO_60s")

    ultimos_10m = historico_cliente(conn, cliente, ts - JANELA_GEO_SEGUNDOS, ts)
    if any(t["city"] != transacao["city"] for t in ultimos_10m):
        motivos.append("GEO_10m")

    return ts, motivos
