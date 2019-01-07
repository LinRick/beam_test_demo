package ParDo_and_Combine;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import Def_function.Mean_Combine_double.Meandoubles;
import Def_function.FtoC_temperature;
import Def_function.Print_DoFn;


public class Mean_double_value {
	
	public static void main(String[] args) {
		
		FtoC_temperature FtoCfun=new FtoC_temperature();
		Meandoubles meanfun=new Meandoubles();
		Print_DoFn prifun=new Print_DoFn();
		
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);	
		
		PCollection<Double> data=p.apply("create 5 data",Create.of(100.0,200.0,300.0,400.0,500.0));		
		PCollection<Double> transform1=data.apply(ParDo.of(FtoCfun));		
		PCollection<Double> mean = transform1.apply("mean tempeature data",Combine.globally(meanfun));					
		PCollection<Void> print_result= mean.apply("print data",ParDo.of(prifun));
		
		p.run();
	}
	
}


