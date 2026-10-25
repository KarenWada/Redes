#!/bin/sh
# Roda o benchmark com 1..MAX clientes simultaneos, REPS vezes cada.
# Uso: ./run_bench.sh <IP> <porta> <segundos> <repeticoes> [bloco] [max_clientes]
# Os resultados (linhas RESULT) aparecem na saida do servidor.

IP=$1
PORT=$2
SECS=$3
REPS=$4
BLOCK=${5:-65536}
MAX=${6:-10}

if [ -z "$REPS" ]; then
    echo "Uso: $0 <IP> <porta> <segundos> <repeticoes> [bloco] [max_clientes]"
    exit 1
fi

for n in $(seq 1 "$MAX"); do
    for r in $(seq 1 "$REPS"); do
        echo "== $n cliente(s), repeticao $r =="
        for i in $(seq 1 "$n"); do
            ./chat_client "$IP" "$PORT" "$SECS" "$BLOCK" &
        done
        wait
        sleep 2
    done
done
