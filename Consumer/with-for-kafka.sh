#!/bin/bash
set -e

host="$1"
shift
cmd="$@"

echo "Esperando a que Kafka esté listo en $host..."

until nc -z ${host%:*} ${host#*:}; do
  >&2 echo "Kafka no disponible aún — esperando..."
  sleep 2
done

>&2 echo "Kafka está disponible — ejecutando comando"
exec $cmd
