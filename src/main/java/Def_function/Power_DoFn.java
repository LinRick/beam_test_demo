package Def_function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;



public class Power_DoFn extends DoFn<Integer, Double> 
{	
	public int times;	
	@ProcessElement
	public void test1(ProcessContext c)
	{				
		Double d=(double) c.element()*times;
		System.out.println("powervalue="+d);
		c.output(d);
	}
}
