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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

import Combine_function.Mean_value.Meandoubles;

public class Variance_value {

	public static class Variancedoubles implements SerializableFunction<Iterable<Double>, Double> {
		  @Override
		  public Double apply(Iterable<Double> input) {
			
			double count=0.0;
			double varsum=0.0;
			double mm=1.0;
			
		    for (double item : input) {
		    	varsum+=Math.pow((item-mean), 2);
		    	count++;
		    }
		    double var=varsum/count;
		    return Math.pow(var,0.5);
		  }
		}


	public static void main(String[] args) {
			PipelineOptions options = PipelineOptionsFactory.create();		
			Pipeline p = Pipeline.create(options);		
			PCollection<Double> a=p.apply("create data",Create.of(1.0,2.0,3.0,4.0,5.0));
			
			PCollection<Double> mean = a.apply("mean data",Combine.globally(new Meandoubles()));
			
			PCollection<Double> meankeydata=mean.apply(ParDo
					.of(new DoFn<Double,Double>(){
				@ProcessElement
				public void test(ProcessContext c){
					System.out.println(c.element());
					double d=c.element();
					c.output(d);					
				}
			}));
			
			PCollectionList<Double> pcs = PCollectionList.of(mean).and(a);
			
			
			 PCollection<Double> pcX = pcs.get(0);
			 PCollection<Double> pcY = pcs.get(1);

			
			
			
			p.run();
			
			
			show.
			
			
			PCollection<Double> var = a.apply("variance data",Combine.globally(new Variancedoubles()));
			
			PCollection<Void> show_var=var.apply(ParDo
					.of(new DoFn<Double,Void>(){
				@ProcessElement
				public void test(ProcessContext c){
					System.out.println(c.element());
					//Double d=c.element();
					//c.output(d);					
				}
			}));
			
			
			

	}

}
