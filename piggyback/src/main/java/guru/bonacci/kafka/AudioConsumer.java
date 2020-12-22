package guru.bonacci.kafka;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.DataLine;
import javax.sound.sampled.SourceDataLine;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AudioConsumer {

	static String TOPIC_NAME = "audio-example2";

	@SuppressWarnings("resource")
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

    	PipedInputStream in = new PipedInputStream();
    	PipedOutputStream out = new PipedOutputStream(in);
    	DataOutputStream data = new DataOutputStream(out);
    	new Thread(
			  new Runnable(){
			    public void run(){
					try {
						while (true) {
							ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10l));
							for (ConsumerRecord<String, String> record : records) {
								try {
									data.write(record.piggybackByte());
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
					} finally {
						System.out.println("Over and out");
						consumer.close();
					}
			    }
			  }
		).start();

        AudioInputStream audioStream = AudioSystem.getAudioInputStream(new BufferedInputStream(in));
        AudioFormat format = audioStream.getFormat();
        DataLine.Info info = new DataLine.Info(SourceDataLine.class, format);
        SourceDataLine audioLine = (SourceDataLine) AudioSystem.getLine(info);
 
        audioLine.open(format);
        audioLine.start();
             
        System.out.println("Playback started.");
             
        byte[] bytesBuffer = new byte[4096];
        int bytesRead = -1;
 
        while ((bytesRead = audioStream.read(bytesBuffer)) != -1) {
            audioLine.write(bytesBuffer, 0, bytesRead);
        }
             
        audioLine.drain();
        audioLine.close();
        audioStream.close();
             
        System.out.println("Playback completed.");
	}
}