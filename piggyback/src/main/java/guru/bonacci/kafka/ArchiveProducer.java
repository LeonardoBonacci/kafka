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

	private Producer<String, String> producer;
	private Long numMessages;
	
	public static void main(final String... args) throws IOException {
		new ArchiveProducer("dostojevski.txt").send("file-example");
//		new ArchiveProducer("dummy.wav").send("audio-example");
	}

	public ArchiveProducer(String filename) throws IOException {
		File file = new File(Thread.currentThread().getContextClassLoader().getResource(filename).getFile());
		FileInputStream inputStream = new FileInputStream(file);
		
		producer = new KafkaProducer<String, String>(configure(), inputStream); // PASS THE FILE TO THE PRODUCER
		// add one to demonstrate that the producer does not break after the entire inputstream has been sent to the broker.
		numMessages = (long)Thread.currentThread().getContextClassLoader().getResourceAsStream(filename).readAllBytes().length + 1;
	}

	private Properties configure() {
		final Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	void send(String topic) {
		System.out.printf("about to start sending %d messages %n", numMessages);

		// Produce sample data
		for (Long i = 0L; i < numMessages; i++) {
			String key = "foo " + i;
			String value = "bar " + i;
			
			System.out.printf("Producing record: %s\t%s%n", key, value);
			producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {
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
		System.out.printf("%d messages were produced to topic %s%n", numMessages, topic);
		producer.close();
	}
}