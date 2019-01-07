package Kafka_streaming_data;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import avro.shaded.com.google.common.collect.ImmutableList;

public class Kafka_IO {

	public static void main(String[] args) {
		
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        // use withTopics(List<String>) to read from multiple topics.
        // PCollection<KV<Long, String>>
        p.apply(KafkaIO.<Long, String>read()
		 .withBootstrapServers("10.236.1.4:9092")
		 .withTopics(ImmutableList.of("topic_a", "topic_b"))		 
		 .withKeyDeserializer(LongDeserializer.class)
		 .withValueDeserializer(StringDeserializer.class)
         .withoutMetadata())
         
         .apply(Values.<String>create()) // PCollection<String>
         .apply(ParDo.of(new DoFn<String, Void>() {
        	 @ProcessElement
        	 public void processElement(ProcessContext ctx) throws Exception {
        		 System.out.printf("read '%s' from kafka", ctx.element());
        	 }
         }));
         
        p.run();


	}

}
