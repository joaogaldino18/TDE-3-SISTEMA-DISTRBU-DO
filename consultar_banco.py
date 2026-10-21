"""Mostra as últimas transações salvas no banco e um resumo das fraudes."""

from database import conectar


def main():
    conn = conectar()
    total = conn.execute("SELECT COUNT(*) FROM transacoes").fetchone()[0]
    fraudes = conn.execute("SELECT COUNT(*) FROM transacoes WHERE fraude = 1").fetchone()[0]
    print(f"Total de transações: {total} | Fraudes: {fraudes}\n")

    for motivo in ("ALTO_VALOR", "TEMPO_60s", "GEO_10m"):
        qtd = conn.execute(
            "SELECT COUNT(*) FROM transacoes WHERE motivos LIKE ?", (f"%{motivo}%",)
        ).fetchone()[0]
        print(f"  {motivo:<11} {qtd}")

    print("\nÚltimas 20 transações:")
    for t in conn.execute("SELECT * FROM transacoes ORDER BY ts_epoch DESC LIMIT 20"):
        print(
            f"  {t['timestamp'][:19]} | {t['client_id']} | R$ {t['amount']:>10.2f} | "
            f"{t['city']:<15} | {t['motivos'] or 'OK'}"
        )


if __name__ == "__main__":
    main()
