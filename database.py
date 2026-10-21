"""Persistência das transações em SQLite."""

import sqlite3

from config import DB_PATH


def conectar(caminho=DB_PATH):
    conn = sqlite3.connect(caminho)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transacoes (
            transaction_id TEXT PRIMARY KEY,
            client_id      TEXT NOT NULL,
            amount         REAL NOT NULL,
            city           TEXT NOT NULL,
            type           TEXT NOT NULL,
            timestamp      TEXT NOT NULL,
            ts_epoch       REAL NOT NULL,
            fraude         INTEGER NOT NULL,
            motivos        TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cliente_ts ON transacoes (client_id, ts_epoch)")
    conn.commit()
    return conn


def historico_cliente(conn, client_id, desde_epoch, ate_epoch):
    """Transações do cliente com ts_epoch no intervalo [desde, ate]."""
    return conn.execute(
        "SELECT * FROM transacoes WHERE client_id = ? AND ts_epoch BETWEEN ? AND ? ORDER BY ts_epoch",
        (client_id, desde_epoch, ate_epoch),
    ).fetchall()


def salvar(conn, transacao, ts_epoch, motivos):
    conn.execute(
        """
        INSERT OR IGNORE INTO transacoes
            (transaction_id, client_id, amount, city, type, timestamp, ts_epoch, fraude, motivos)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            transacao["transaction_id"],
            transacao["client_id"],
            transacao["amount"],
            transacao["city"],
            transacao["type"],
            transacao["timestamp"],
            ts_epoch,
            1 if motivos else 0,
            ",".join(motivos) or None,
        ),
    )
    conn.commit()
