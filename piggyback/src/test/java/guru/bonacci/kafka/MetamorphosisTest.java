package guru.bonacci.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

public class MetamorphosisTest {

	@Test
	void inputOutputComparison() throws IOException, InterruptedException {
		String topic = "metamorphosis" + new Random().nextInt(1000);

		String in = "kafka.txt";
		String out = "kafka-copy.txt";
		
		new ArchiveProducer(in).send(topic);
		new FileConsumer(in, out).receive(topic);

		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		
		File originalFile = new File(classloader.getResource(in).getFile());
	    String originalString = Files.asCharSource(originalFile, Charsets.UTF_8).read(); 
		String originalSha = Hashing.sha256()
				  .hashString(originalString, StandardCharsets.UTF_8)
				  .toString();		

		Thread.sleep(3141); // and breath...
		
		File resultFile = new File(classloader.getResource(out).getFile());
	    String resultString = Files.asCharSource(resultFile, Charsets.UTF_8).read(); 
		String resultSha = Hashing.sha256()
				  .hashString(resultString, StandardCharsets.UTF_8)
				  .toString();		
		
		assertEquals(originalSha, resultSha); // Voilà
		
		MoreFiles.deleteRecursively(Paths.get("kafka-copy.txt"), RecursiveDeleteOption.ALLOW_INSECURE);
	}
}