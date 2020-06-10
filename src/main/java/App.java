import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class App {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(properties);
        Producer<Long, String> producer = ProducerCreator.createProducer(properties);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        MessageReceiver<Long, String> messageReceiver =
                new MessageReceiver<>(consumer, IKafkaConstants.TOPIC_NAME, countDownLatch);
        MessageSender<Long, String> messageSender =
                new MessageSender<>(producer, "My name is Ruli", countDownLatch, IKafkaConstants.TOPIC_NAME);


        Thread thread1 =new Thread(messageReceiver);
        Thread thread2 =new Thread(messageSender);

        thread1.start();
        thread2.start();

        Thread.sleep(5000);

        thread1.interrupt();
        thread2.interrupt();

        consumer.close();

    }
}
