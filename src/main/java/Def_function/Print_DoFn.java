package Def_function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

public class Print_DoFn extends DoFn<Double, Void> 
{
	
		
	@ProcessElement
	public void test2(ProcessContext c)
	{				
		double d2=c.element();
		System.out.println("================Result================");
		System.out.println("Value="+Math.round(d2));		
	}
	//Integer sol=d2;	

}
