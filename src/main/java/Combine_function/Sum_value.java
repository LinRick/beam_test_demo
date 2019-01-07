package Combine_function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

public class Sum_value {

	public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
		  @Override
		  public Integer apply(Iterable<Integer> input) {
		    int sum = 0;
		    for (int item : input) {
		      sum += item;
		    }
		    return sum;
		  }
		}


	public static void main(String[] args) {
			PipelineOptions options = PipelineOptionsFactory.create();		
			Pipeline p = Pipeline.create(options);		
			PCollection<Integer> a=p.apply("create data",Create.of(1,2,3,4,5,6,7,8,9,10));
			
			PCollection<Integer> sum1 = a.apply("sum data",Combine.globally(new SumInts()));
			//PCollection<Integer> sum1 = a.apply("sum data",Sum.integersGlobally());
			
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
