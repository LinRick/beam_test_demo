package ParDo_function;

import java.util.IdentityHashMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ParDo_keyvalue {

	public static void main(String[] args) {
		int[] value=new int[] {1,2,3,4,5,6,7,8,9,10};		
				
		IdentityHashMap<String, Integer> map = new IdentityHashMap<String, Integer>();		
				  
		for (int i=0; i<value.length; i++)
	    {
	      if (value[i]<=5)
	      {
	    	  map.put(new String("M1"), value[i]);
	      }
	      else
	      {
	    	  map.put(new String("M2"), value[i]);
	      }
	    }		  
		System.out.println(map);
		
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);		
		PCollection<KV<String, Integer>> t=p.apply("create data",Create.of(map));
		
		
		
		PCollection<KV<String, Integer>> b=t.apply(ParDo
				.of(new DoFn<KV<String, Integer>,KV<String, Integer>>(){
			@ProcessElement
			//@Override
			public void test(ProcessContext c){				
				KV<String, Integer> k=null;
				if (c.element().getValue()<=5)
				{
					//map1.put(new String("M1"), c.element().getValue());
					k=KV.of(c.element().getKey(), c.element().getValue()+1);
			    }
			    else
			    {
			    	k=KV.of(c.element().getKey(), c.element().getValue()+1);
			    	//map1.put(new String("M2"), c.element().getValue());
			    }
				
				c.output(k);
				System.out.println(k);
			}
		}));
		
		p.run();
		

	}

}
