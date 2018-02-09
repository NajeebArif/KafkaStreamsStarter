/**
 * 
 */
package narif.poc.kafka.kafkastreamsstarter;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * @author narif
 *
 */
public class StreamingWordCounter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-word-count");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, String> source = builder.stream("stream-source-topic");
		source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
				.groupBy((key, value) -> value)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-store")).toStream()
				.to("stream-sink-topic", Produced.with(Serdes.String(), Serdes.Long()));

		final Topology topology = builder.build();
		System.out.println(topology.describe());
		final KafkaStreams stream = new KafkaStreams(topology, props);

		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread("Shutdown-Word-Counter") {
			@Override
			public void run() {
				stream.close();
				latch.countDown();
			}
		});

		try {
			latch.await();
			stream.start();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
