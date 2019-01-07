package Batch_data;


import java.util.IdentityHashMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Beam_batchdata_JSON_keyvalue {

	public static void main(String[] args) {		
				
		int[] value=new int[] {1,2,3,4,5,6,7,8,9,10};		
		
		
		String message;
		JSONObject json = new JSONObject();
		
				  
		for (int i=0; i<value.length; i++)
	    {
	      if (value[i]<=5)
	      {
	    	  json.put(new String("M1"), value[i]);
	      }
	      else
	      {
	    	  json.put(new String("M2"), value[i]);
	      }
	    }
		message = json.toString();
		System.out.println(message);

		
		//pipeline - start
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);		
		//(key, value):{(M1,1), (M1,2),...,(M2,10)} - paired key/value
		
		//PCollection<KV<String, Integer>> t=p.apply("create data",Create.of(message));
		
		//Aggregation operation: sum, average
		//PCollection<KV<String, Integer>> sum1 = t.apply("sum data",Sum.<String>integersPerKey());		
		//PCollection<KV<String, Double>> sum1 = t.apply("sum data",Mean.<String, Integer>perKey()); //ok
		
		//PCollection<String> sum_str = sum1.apply("int_to_str data",ToString.elements());		

		//sum_str.apply("write_to_txt",TextIO.write()
		//		.to("/root/java_beam_test/sumint.txt"));

		
				
		//.withSuffix(".txt")
		//.withNumShards(1);
		
		p.run();

		
	}

}
