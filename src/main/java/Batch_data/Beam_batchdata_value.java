package Batch_data;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Beam_batchdata_value {

	public static void main(String[] args) {		
		
		//pipeline - start
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);		
		
		PCollection<Integer> a=p.apply("create data",Create.of(1,2,3,4,5,6,7,8,9,10));
		
		PCollection<Integer> sum1 = a.apply("sum data",Sum.integersGlobally());		
		
		
		PCollection<String> sum_str = sum1.apply("int_to_str data",ToString.elements());		

		sum_str.apply("write_to_txt",TextIO.write()
				.to("/home/ccma/")
                .withSuffix("123.txt"));

		
		p.run();

		/*
		PCollection<String> sum_str = sum1.apply("int_to_str data",
		MapElements.into(TypeDescriptors.strings()).
		via((int num) -> Integer.toString(num)));
		 */
		
		//sum1.getCoder()
		/*
		p.apply("read_from_txt",TextIO.read()
				.from("/root/java_beam_test/textfile.txt")
				)		
		*/
		/*
		PCollection<String> s=sum1.apply("write_to_txt",TextIO.write
				.to("/root/java_beam_test/sumint")
				.withSuffix(".txt")
				.withNumShards(1)
				);
		*/
		
		
		
				
		System.out.println(sum1.toString());
		System.out.println(sum1.hashCode());
		System.out.println(sum1.getCoder());
		System.out.println(sum1.getPipeline());
		System.out.println(sum1.getName());
		System.out.println(sum1.getCoder());
		

		
		/*Note that coders are unrelated to parsing or formatting data
		when interacting with external data sources or sinks.
		Such parsing or formatting should typically be done explicitly, 
		using transforms such as ParDo or MapElements.
		*/
	}

}
