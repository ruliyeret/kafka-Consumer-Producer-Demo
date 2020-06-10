import kafka.common.UnknownTopicOrPartitionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaManager {

    private AdminClient admin;

    public  KafkaManager(AdminClient admin){
        this.admin = admin;

    }
    private ConsumerCreator consumerCreator;
    private ProducerCreator producerCreator;

    public void recreateTopics(final int numPartition){

    }

    public ProducerCreator getProducerCreator() {
        return producerCreator;
    }

    public ConsumerCreator getConsumerCreator() {
        return consumerCreator;
    }

    public void deleteTopic(final List<String> topicsToDelete)
            throws InterruptedException, ExecutionException {
        try {

            this.admin.deleteTopics(topicsToDelete).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
            System.out.println("Encountered exception during topic deletion: " + e.getCause());
        }
        System.out.println("Deleted old topics: " + topicsToDelete);
    }

    public void createTopic(final String topic, int numPartitions, short replicationFactor){
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        this.admin.createTopics(Collections.singleton(newTopic));
    }

    public boolean topicExist(String topic) throws ExecutionException, InterruptedException {
        return this.admin.listTopics().names().get().contains(topic);
    }

    public Set<String> getTopicList() throws ExecutionException, InterruptedException {
        return  this.admin.listTopics().names().get();
    }



}
