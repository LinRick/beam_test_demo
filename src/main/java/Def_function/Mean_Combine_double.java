package Def_function;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class Mean_Combine_double 
{
	public static class Meandoubles implements SerializableFunction<Iterable<Double>, Double> 
	{
		public Double apply(Iterable<Double> input) 
		{
			double sum = 0.0;
			double count=0.0;
			for (double item : input) 
			{
				sum += item;
				count++;
			}
			double mean=sum/count;
			return mean;
		}
	}
}




