package storm.etl.log;

/**
 * Created by endy on 14-9-25.
 */
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

public class ConsumerSample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "10.132.174.98:2181");
        props.put("zookeeper.connection.timeout.ms", "1000000");
        props.put("group.id", "storm_group");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("fks", 1);
        Map<String, List<KafkaStream<byte[], byte[]>>>   topicMessageStreams =
                consumerConnector.createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get("fks");

        ExecutorService executor = Executors.newFixedThreadPool(1);

        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new Runnable() {
                public void run() {
                    for (MessageAndMetadata msgAndMetadata : stream) {
                        System.out.println("topic: " + msgAndMetadata.topic());
                        Message message = (Message) msgAndMetadata.message();
                        ByteBuffer buffer = message.payload();
                        byte[] bytes = new byte[message.payloadSize()];
                        buffer.get(bytes);
                        String tmp = new String(bytes);
                        System.out.println("message content: " + tmp);
                    }
                }
            });
        }

    }
}

