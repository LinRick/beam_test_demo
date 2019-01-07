package Def_function;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class Sum_Combine_Int implements SerializableFunction<Iterable<Double>, Double> 
{
	/*public void print()
	  {
		  System.out.println("sumvalue=");
	  }
	*/
	public int add;
	@Override
	public Double apply(Iterable<Double> input) 
	{
		Double sum = 0.0;
		for (double item : input) 
		{
	      sum += item;	      
	    }
		return sum+add;
	}	  
}


