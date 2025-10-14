#!/bin/sh
set -e

host=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

echo "🕒 Waiting for Kafka to be ready at $host ..."
until nc -z $(echo $host | cut -d: -f1) $(echo $host | cut -d: -f2); do
  sleep 2
done

echo "✅ Kafka is up, starting producer..."
exec "$@"
