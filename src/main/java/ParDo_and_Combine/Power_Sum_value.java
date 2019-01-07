package ParDo_and_Combine;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import Combine_function.Mean_value.Meandoubles;
import Def_function.Power_DoFn;
import Def_function.Sum_Combine_Int;
import Def_function.Var_Combine_Double;
import Def_function.Print_DoFn;



public class Power_Sum_value {

	public static void main(String[] args) {
		Power_DoFn powfun=new Power_DoFn();
		powfun.times=5;
		
		Print_DoFn prifun=new Print_DoFn();
		
		Sum_Combine_Int sumfun=new Sum_Combine_Int();
		sumfun.add=5;
		
		Var_Combine_Double varfun=new Var_Combine_Double();
		varfun.mean=7.5;
		
		
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);	
		
		PCollection<Integer> a=p.apply("create data",Create.of(1,2));
		
		PCollection<Double> b=a.apply(ParDo.of(powfun));
		
		PCollection<Double> mean = b.apply("mean data",Combine.globally(sumfun));
		
		PCollection<Double> var = b.apply("var data",Combine.globally(varfun));
		
		PCollection<Void> show_mean = mean.apply("mean data",ParDo.of(prifun));
		
		PCollection<Void> show_var= var.apply("mean data",ParDo.of(prifun));
		
		
		//Sum_Combine_Int test=new Sum_Combine_Int();
		//test.print();
		
		p.run();

	}

}
