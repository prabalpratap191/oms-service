#!/usr/bin/env bash
# Kafka admin helpers for the local cluster (namespace oms). Usage: ./scripts/kafka-commands.sh <command> [args]
set -euo pipefail
NS=${NS:-oms}
BIN=/opt/kafka/bin
K() { kubectl -n "$NS" exec deploy/kafka -- "$@"; }
BS="--bootstrap-server localhost:9092"
PARTITIONS=${PARTITIONS:-3}; REPLICAS=${REPLICAS:-1}

# Keep in sync with com.oms.kafka.KafkaTopics (the app also creates these on startup via KafkaAdmin)
TOPICS=(
  oms.wms.inventoryCheck oms.wms.inventoryReserve oms.wms.inventoryRelease oms.wms.returnInspection
  oms.payment.paymentCapture oms.payment.paymentRefund oms.payment.paymentReconcile
  oms.logistics.shipmentCreate oms.logistics.shipmentUpdate oms.logistics.shipmentCancel oms.logistics.returnPickup
  oms.finance.creditMemo oms.notification.orderLifecycle oms.marketplace.orderIngestResult
  marketplace.oms.orderIngest wms.oms.inventoryCheckResult wms.oms.inventoryReserveResult wms.oms.inventoryReleaseResult
  wms.oms.returnInspectionResult payment.oms.paymentCaptureResult payment.oms.paymentRefundResult payment.oms.paymentReconcileResult
  logistics.oms.shipmentCreateResult logistics.oms.shipmentStatus logistics.oms.shipmentCancelResult logistics.oms.returnPickupResult
  finance.oms.creditMemoAck
)
# Keep in sync with com.oms.kafka.KafkaGroups
GROUPS_=(oms-saga-group oms-shipment-group oms-rma-group oms-ingest-group oms-stub-wms-group oms-stub-payment-group oms-stub-logistics-group oms-stub-finance-group)

case "${1:-help}" in
  create-topics)   for t in "${TOPICS[@]}"; do K $BIN/kafka-topics.sh $BS --create --if-not-exists --topic "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICAS"; done ;;
  list-topics)     K $BIN/kafka-topics.sh $BS --list ;;
  describe-topic)  K $BIN/kafka-topics.sh $BS --describe --topic "$2" ;;
  delete-topics)   for t in "${TOPICS[@]}"; do K $BIN/kafka-topics.sh $BS --delete --if-exists --topic "$t"; done ;;
  list-groups)     K $BIN/kafka-consumer-groups.sh $BS --list ;;
  describe-groups) for g in "${GROUPS_[@]}"; do K $BIN/kafka-consumer-groups.sh $BS --describe --group "$g" || true; done ;;
  lag)             K $BIN/kafka-consumer-groups.sh $BS --describe --all-groups ;;
  # Consumer groups cannot be "created" explicitly in Kafka: they appear when the first consumer joins.
  # Reset a stopped group (scale oms-service to 0 first):  reset-offsets <group> <topic> earliest|latest
  reset-offsets)   K $BIN/kafka-consumer-groups.sh $BS --group "$2" --topic "$3" --reset-offsets --to-"${4:-latest}" --execute ;;
  consume)         K $BIN/kafka-console-consumer.sh $BS --topic "$2" --from-beginning --max-messages "${3:-10}" ;;
  produce)         K sh -c "echo '$3' | $BIN/kafka-console-producer.sh $BS --topic $2" ;;
  *) echo "commands: create-topics list-topics describe-topic <t> delete-topics list-groups describe-groups lag reset-offsets <g> <t> [earliest|latest] consume <t> [n] produce <t> '<json>'" ;;
esac
