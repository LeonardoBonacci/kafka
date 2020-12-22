package guru.bonacci.kafka;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class FileConsumer {

	static String TOPIC_NAME = "file-example";

	public static void main(final String[] args) throws Exception {

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(TOPIC_NAME));

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		int bytesInOutputfile = classLoader.getResourceAsStream("dostojevski.txt").readAllBytes().length;

		System.out.printf("%d bytes in file %n", bytesInOutputfile);
		ByteBuffer buffer = ByteBuffer.allocate(bytesInOutputfile);

		try {
			boolean escape = false;
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10l));
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("Consumed record with key %s and value %s...%n", record.key(),
							record.value());
					System.out.printf("... and piggyback byte %d%n", record.piggybackByte());

					escape = buffer.position() >= bytesInOutputfile;
					if (escape) continue;
					buffer.put(record.piggybackByte());
				}
				if (escape) break;
			}
		} finally {
			FileOutputStream fos = new FileOutputStream("src/main/resources/dostojevski-copy.txt");
			fos.write(buffer.array());

			System.out.println("Over and out");
			fos.close();
			consumer.close();
		}
	}
}