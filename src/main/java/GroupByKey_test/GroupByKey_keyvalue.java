package GroupByKey_test;

import java.util.IdentityHashMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GroupByKey_keyvalue {

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
		
		PCollection<KV<String, Iterable<Integer>>> groupedmachines = t.apply(
			    GroupByKey.<String, Integer>create());
		
		PCollection<KV<String, Iterable<Integer>>> ans=groupedmachines.apply(ParDo.
				of(new DoFn<KV<String, Iterable<Integer>>,KV<String, Iterable<Integer>>>(){
			@ProcessElement
			public void test1(ProcessContext c){
				System.out.println(c.element().getValue());
				c.output(c.element());
			}
		}));
			
		p.run();
	}

}
