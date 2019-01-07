package Def_function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

public class FtoC_temperature extends DoFn<Double, Double> {
	double times=5.0/9;	
	@ProcessElement
	public void test1(ProcessContext f)
	{	
		double c= (f.element()-32)*times;
		System.out.println("C_temperature="+Math.round(c));
		f.output(c);
	}
}


