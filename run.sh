#!/bin/bash
# run.sh

# Función para manejar la señal de SIGTERM
graceful_shutdown() {
    echo "Recibida señal de apagado. Cerrando aplicaciones..."
    pkill -f "python producer.py"
    pkill -f "python consumer.py"
    exit 0
}

# Registrar manejador de señales
trap 'graceful_shutdown' SIGTERM SIGINT

# Ejecutar producer y consumer en paralelo
python producer.py &
python consumer.py &

# Mantener el script corriendo hasta recibir señal
wait
