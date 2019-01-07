package Combine_function;

import java.util.Collection;
import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

import Combine_function.Sum_value.SumInts;

public class max2min_value {
	
	public static class maxminDoubles implements SerializableFunction<Iterable<Integer>, , Double> {
		  @Override
		  public Double apply(Iterable<Integer> input) {
			double mid = 0.0;
		    
		    Collection<Integer> collection =  (Collection<Integer>) input;
		        
		    double max_number = Collections.max(collection)+0.5;
		    //Integer min_number = Collections.min(collection);
		    //mid=(max_number+min_number)/2;
		    mid=5.0;
		    return max_number;
		  }
		}
	public static void main(String[] args) {
			PipelineOptions options = PipelineOptionsFactory.create();		
			Pipeline p = Pipeline.create(options);		
			PCollection<Integer> a=p.apply("create data",Create.of(1,2,3,4,5,6,7,8,9,10));
			
			PCollection<Double> sum1 = a.apply("sum data",Combine.globally((GlobalCombineFn<? super InputT, ?, OutputT>) new maxminDoubles()));
			//PCollection<Integer> sum1 = a.apply("sum data",Sum.integersGlobally());
			

			PCollection<Double> ans=sum1.apply(ParDo
					.of(new DoFn<Double,Double>(){
				@ProcessElement
				public void test(ProcessContext c){				
					double d=c.element();
					System.out.println(d);
					c.output(d);					
				}
			}));

			
			p.run();

	}

}
