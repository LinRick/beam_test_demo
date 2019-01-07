package Flatten_and_Partition;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import Combine_function.Sum_value.SumInts;

public class Flatten_value {

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);		
		
		PCollection<Integer> a=p.apply("create data",Create.of(1,2,3));
		PCollection<Integer> b=p.apply("create data",Create.of(4,5,6));
		PCollection<Integer> c=p.apply("create data",Create.of(7,8,9,10));
		PCollectionList<Integer> pcs = PCollectionList.of(a).and(b).and(c);
		
		PCollection<Integer> firstPc = pcs.get(0); //take first PCollection
		PCollection<Integer> merged = pcs.apply(Flatten.pCollections());
	
		PCollection<Integer> sum1 = merged.apply("sum data",Sum.integersGlobally());
		
		PCollection<Integer> ans=sum1.apply(ParDo
				.of(new DoFn<Integer,Integer>(){
			@ProcessElement
			public void test(ProcessContext c){				
				int d=c.element();
				System.out.println(d);
				c.output(d);					
			}
		}));

		p.run();
	}

	
}

