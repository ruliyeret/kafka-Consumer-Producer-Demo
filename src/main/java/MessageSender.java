import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class MessageSender<K, V> implements Runnable{

    private final Producer<K, V> producer;
    private final V message;
    private final CountDownLatch latch;
    private final String topic;


    MessageSender(Producer<K ,V> producer, V message, CountDownLatch countDownLatch, String topic){
        this.producer = producer;
        this.message = message;
        this.latch = countDownLatch;
        this.topic = topic;
    }




    public void start() {
        try {
            for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
                ProducerRecord<K, V> record =
                        new ProducerRecord<>(this.topic, message);
                try {
                    String messageStr = "Message_" +  index;
                    long startTime = System.currentTimeMillis();
                    RecordMetadata metadata = producer.send(record,
                            new ProducerCallBack(startTime, index, messageStr)).get();

                    System.out.println("Record sent with key " + index +
                            " to partition " + metadata.partition()
                            + " with offset " + metadata.offset());

                } catch (ExecutionException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }
            }
        } finally {
            latch.countDown();
//            producer.flush();
//            producer.close();
        }
    }

    @Override
    public void run() {
        this.start();
    }
}
