package Combine_function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;

import Combine_function.Sum_value.SumInts;

public class Average_value {

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
	
	public class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
		
		public class Accum {
			int sum = 0;
			int count = 0;
			}
		
		@Override
		public Accum createAccumulator() {
			return new Accum();
			}
		
		@Override
		public Accum addInput(Accum accum, Integer input) {
			accum.sum += input;
			accum.count++;
			return accum;
			}

		@Override
		public Accum mergeAccumulators(Iterable<Accum> accums) {
			Accum merged = createAccumulator();
			for (Accum accum : accums) {
				merged.sum += accum.sum;
				merged.count += accum.count;
				}
			return merged;
			}

		@Override
		public Double extractOutput(Accum accum) {
			return ((double) accum.sum) / accum.count;
			}
		}
	
	
	public static void main(String[] args) {
			
		PipelineOptions options = PipelineOptionsFactory.create();		
			Pipeline p = Pipeline.create(options);		
			
			PCollection<Integer> a=p.apply("create data",Create.of(1,2,3,4,5,6,7,8,9,10));
			PCollection<Double> average = a.apply(Combine.globally(new AverageFn()));								
			//PCollection<Integer> sum1 = a.apply("sum data",Sum.integersGlobally());
			

			PCollection<Double> ans=average.apply(ParDo
					.of(new DoFn<Double,Double>(){
				@ProcessElement
				public void test(ProcessContext c){				
					double d=c.element();
					System.out.println(d);
					//c.output(d);					
				}
			}));

			
			p.run();

	}

}
