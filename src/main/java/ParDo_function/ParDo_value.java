package ParDo_function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;

public class ParDo_value {
	
	public static void main(String[] args) {
	
					
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);	

		//PCollection<String> a=p.apply("create data",Create.of("ab","cde"));
		/*
		PCollection<String> b=a.apply(ParDo.of(new DoFn<String,String>(){
			@ProcessElement
			public void processElement(ProcessContext c){
				c.output(c.element().toUpperCase());
				System.out.println("mapper");
			}
		}));
		PCollection<Integer> d=b.apply(ParDo.of(new DoFn<String,Integer>(){
			@ProcessElement
			public void processElement(ProcessContext c){
				c.output(c.element().length());
				System.out.println("reducer");
			}
		}));
		*/
		PCollection<Integer> a=p.apply("create data",Create.of(1,2));
		
		PCollection<Integer> b=a.apply(ParDo
				.of(new DoFn<Integer,Integer>(){
			@ProcessElement
			//@Override
			public void test(ProcessContext c){				
				int d=c.element()+1;
				c.output(d);
				//System.out.println(d);
			}
		}));
		

		PCollection<Integer> ans=b.apply(ParDo.of(new DoFn<Integer,Integer>(){
			@ProcessElement
			public void test1(ProcessContext c){				
				int d1=c.element()*2;
				//c.output(d1);
				System.out.println(d1);
			}
		}));

		
		/*fail
		PCollection<Integer> ans=b.apply(Combine.globally(new <Integer,Integer>(){
			@Override
			public int apply(int c){				
				int sum=0;
				for (int value:c){
					sum+=value;
				}
				System.out.println(sum);
				return sum;
			}
		}));
		*/
				
				
		
		//PCollection<Integer> a=p.apply("create data",Create.of(1,2,3,4,5,6,7,8,9,10));
		
				
		//PCollection<Integer> sum1 = a.apply("sum data", ParDo.of(new Employee()));
		
		
		//PCollection<String> sum_str = sum1.apply("int_to_str data",ToString.elements());		

		//sum_str.apply("write_to_txt",TextIO.write().to("/root/java_beam_test/sumint.txt"));


		p.run();


	}

}
