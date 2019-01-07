package Def_function;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

import Combine_function.Average_value.AverageFn;



public class Var_Combine_Double implements SerializableFunction<Iterable<Double>, Double> 
{
	public double mean;
	@Override	
	public Double apply(Iterable<Double> input) 
	{		
		double count=0.0;
		double varsum=0.0;		
		
	    for (Double item : input) {
	    	varsum+=Math.pow((item-mean), 2);
	    	count++;
	    }
	    double var=varsum/count;
	    return Math.pow(var,0.5);
	 }
	
}

