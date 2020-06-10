public interface IKafkaConstants {
      String KAFKA_BROKERS = "localhost:9092";
      Integer MESSAGE_COUNT=50;
      String CLIENT_ID="client1";
      String TOPIC_NAME="ruli_topic";
      String GROUP_ID_CONFIG="consumerGroup1";
      Integer MAX_NO_MESSAGE_FOUND_COUNT=10;
      String OFFSET_RESET_LATEST="latest";
      String OFFSET_RESET_EARLIER="earliest";
      Integer MAX_POLL_RECORDS=1;
}