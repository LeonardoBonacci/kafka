package guru.bonacci.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ArchiveProducer {

	static String TOPIC_NAME = "audio-example2";
	private static String FILE_NAME = "dummy.wav";
			
	public static void main(final String[] args) throws IOException {
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		File file = new File(classLoader.getResource(FILE_NAME).getFile());
		FileInputStream dostojevski = new FileInputStream(file);
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props, dostojevski); // PASS THE FILE TO THE PRODUCER

		// add one to demonstrate that the producer does not break after the entire inputstream has been sent to the broker.
		Long numMessages = (long)classLoader.getResourceAsStream(FILE_NAME).readAllBytes().length + 1;
		System.out.printf("about to start sending %d messages %n", numMessages);

		// Produce sample data
		for (Long i = 0L; i < numMessages; i++) {
			String key = "foo" + i;
			String value = "bar" + i;
			
			System.out.printf("Producing record: %s\t%s%n", key, value);
			producer.send(new ProducerRecord<String, String>(TOPIC_NAME, key, value), new Callback() {
				@Override
				public void onCompletion(RecordMetadata m, Exception e) {
					if (e != null) {
						e.printStackTrace();
					} else {
						System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(),
								m.partition(), m.offset());
					}
				}
			});
		}

		producer.flush();

		System.out.printf("%d messages were produced to topic %s%n", numMessages, TOPIC_NAME);

		dostojevski.close();
		producer.close();
	}
}