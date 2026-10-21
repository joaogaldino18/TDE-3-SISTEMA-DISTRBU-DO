"""Testes das regras de fraude (python -m unittest)."""

import unittest
from datetime import datetime, timedelta, timezone

from database import conectar, salvar
from fraude import detectar

BASE = datetime(2025, 10, 21, 12, 0, 0, tzinfo=timezone.utc)
_contador = 0


def tx(segundos, cliente="C001", amount=100.0, city="Curitiba"):
    global _contador
    _contador += 1
    return {
        "transaction_id": f"t{_contador}",
        "client_id": cliente,
        "amount": amount,
        "city": city,
        "type": "PIX",
        "timestamp": (BASE + timedelta(seconds=segundos)).isoformat(),
    }


class TestFraude(unittest.TestCase):
    def setUp(self):
        self.conn = conectar(":memory:")

    def processar(self, transacao):
        ts, motivos = detectar(self.conn, transacao)
        salvar(self.conn, transacao, ts, motivos)
        return motivos

    def test_transacao_normal(self):
        self.assertEqual(self.processar(tx(0)), [])

    def test_alto_valor(self):
        self.assertEqual(self.processar(tx(0, amount=10000)), ["ALTO_VALOR"])
        self.assertEqual(self.processar(tx(100, cliente="C002", amount=9999.99)), [])

    def test_tempo_60s(self):
        for s in (0, 10, 20):
            self.assertNotIn("TEMPO_60s", self.processar(tx(s)))
        self.assertIn("TEMPO_60s", self.processar(tx(30)))

    def test_tempo_fora_da_janela(self):
        for s in (0, 30, 50):
            self.processar(tx(s))
        self.assertNotIn("TEMPO_60s", self.processar(tx(60)))

    def test_tempo_outro_cliente_nao_conta(self):
        for s in (0, 10, 20):
            self.processar(tx(s, cliente="C002"))
        self.assertNotIn("TEMPO_60s", self.processar(tx(30)))

    def test_geo_10m(self):
        self.processar(tx(0, city="Curitiba"))
        self.assertEqual(self.processar(tx(300, city="Recife")), ["GEO_10m"])

    def test_geo_fora_da_janela(self):
        self.processar(tx(0, city="Curitiba"))
        self.assertEqual(self.processar(tx(601, city="Recife")), [])

    def test_geo_mesma_cidade(self):
        self.processar(tx(0, city="Curitiba"))
        self.assertEqual(self.processar(tx(300, city="Curitiba")), [])


if __name__ == "__main__":
    unittest.main()
