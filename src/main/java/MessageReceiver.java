import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import kafka.utils.ShutdownableThread;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class MessageReceiver<K, V> extends ShutdownableThread  {

    private final Consumer consumer;
    private final String topic;
    private final CountDownLatch latch;



    MessageReceiver(Consumer<K, V> consumer, String topic, CountDownLatch latch) {
        super("KafkaConsumerExample", false);
        this.consumer = consumer;
        this.topic = topic;
        this.latch = latch;
    }

    public void start() {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("received message : from partition " +
                    record.partition() +
                    ", (" + record.key() +
                    ", " + record.value() +
                    ") at offset " + record.offset());

        }
        if(records.count() == 0){
            System.out.println("finished reading messages");
            latch.countDown();
        }

    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        this.start();
    }


}

