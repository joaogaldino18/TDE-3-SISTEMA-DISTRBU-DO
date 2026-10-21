"""Configurações compartilhadas entre produtor e consumidor."""

KAFKA_BOOTSTRAP = "localhost:9092"
TOPICO = "transacoes"
GRUPO_CONSUMIDOR = "detector-fraude"

DB_PATH = "transacoes.db"

# Intervalo entre transações (segundos)
INTERVALO_SEGUNDOS = 3

# Regras de fraude
LIMITE_ALTO_VALOR = 10000
JANELA_TEMPO_SEGUNDOS = 60
QTD_TRANSACOES_TEMPO = 4
JANELA_GEO_SEGUNDOS = 10 * 60
