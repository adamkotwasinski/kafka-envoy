package envoy;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEnvoy {

    /**
     *                                  +----------+      +--------+
     *                         +------->| CLUSTER1 +----->|CONSUMER|
     *                         |        | APPLES   |      +--------+
     *                         |        +----------+
     *                         |
     * +--------+       +------++       +----------+      +--------+
     * |PRODUCER+------>| ENVOY +------>| CLUSTER2 +----->|CONSUMER|
     * +--------+       +------++       | BANANAS  |      +--------+
     *                         |        +----------+
     *                         |
     *                         |        +----------+      +--------+
     *                         +------->| CLUSTER3 +----->|CONSUMER|
     *                                  | CHERRIES |      +--------+
     *                                  +----------+
     */

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEnvoy.class);

    private static final String ENVOY = "localhost:19092";

    private static final String[] TOPICS = new String[] { "apples", "bananas", "cherries" };
    private static final String[] CLUSTERS = new String[] { "localhost:9092", "localhost:9093", "localhost:9094" };

    private static final Random RANDOM = new Random();

    public static void main(final String[] args)
            throws Exception {

        final Map<String, byte[]> sent = new TreeMap<>();

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ENVOY);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
        try {
            for (final String topic : TOPICS) {
                final byte[] value = new byte[128 * 1024];
                RANDOM.nextBytes(value);
                final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, 0, null, value);
                final RecordMetadata recordMetadata = producer.send(record).get();
                sent.put(topic, value);
                LOG.info("Record for topic [{}] saved at offset {}", topic, recordMetadata.offset());
            }
        }
        finally {
            producer.close();
        }

        for (int i = 0; i < 3; ++i) {
            final String topic = TOPICS[i];
            final byte[] expectedValue = sent.get(topic);

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTERS[i]);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
            try {
                consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
                while (true) {
                    boolean found = false;
                    final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                    for (final ConsumerRecord<String, byte[]> record : records) {
                        if (Arrays.equals(record.value(), expectedValue)) {
                            LOG.info("Matching record for topic [{}] found at offset {}", topic, record.offset());
                            found = true;
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
            }
            finally {
                consumer.close();
            }
        }

    }

}
