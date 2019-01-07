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

public class Mean_value {

	public static class Meandoubles implements SerializableFunction<Iterable<Double>, Double> {
		  public Double apply(Iterable<Double> input) {
			double sum = 0.0;
			double count=0.0;
		    for (double item : input) {
		      sum += item;
		      count++;
		    }
		    double mean=sum/count;
		    return mean;
		  }
		}


	public static void main(String[] args) {
			PipelineOptions options = PipelineOptionsFactory.create();		
			Pipeline p = Pipeline.create(options);		
			PCollection<Double> a=p.apply("create data",Create.of(1.0,2.0,3.0,4.0,5.0));
			
			PCollection<Double> sum1 = a.apply("sum data",Combine.globally(new Meandoubles()));
			//PCollection<Integer> sum1 = a.apply("sum data",Sum.integersGlobally());
			
			PCollection<Void> ans=sum1.apply(ParDo
					.of(new DoFn<Double,Void>(){
				@ProcessElement
				public void test(ProcessContext c){
					System.out.println(c.element());
					//Double d=c.element();
					//c.output(d);					
				}
			}));
			
			
			p.run();

	}

}
