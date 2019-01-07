package GroupByKey_test;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV.OrderByKey;

public class KV_orderbykey {

	public static void main(String[] args) {
		int[] key=new int[] {2,1,3,4,5};
		double[] value=new double[] {1.0,1.0,1.0,1.0,1.0};
		List<KV<Integer, Double>> KVlist = new ArrayList<>();
		List<KV<Integer, Double>> KVtest = new ArrayList<>();
		
		int n=value.length;		
		for (int i=0; i<n; i++){			
			KVlist.add(KV.of(i, value[i]));
			//KV.OrderByKey<Comparable<? super K>, V>;			
			System.out.println(KVlist.get(i));
	    }
		/*
		for (int i=0; i<n; i++){			
			KVtest.add(KV.of(2, value[i]));
			//KV.OrderByKey<Comparable<? super K>, V>;			
			System.out.println(KVtest.get(i));
	    }
		*/
		
		
		KV.of(i, value[i])
		
		
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);		
		PCollection<KV<Integer, Double>> t1=p.apply("create data",Create.of(KVlist));
		
		PCollection<KV<Integer, Double>> t2 = t1.apply(KV.OrderByKey.<Integer, Double>());
				
		KV.OrderByKey<Comparable<? super K>, V>
		//KV.OrderByKey;

	}

}
