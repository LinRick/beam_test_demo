package Def_function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;


import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import javafx.util.Pair;


import org.joda.time.Instant;
import org.joda.time.MutableDateTime;

import avro.shaded.com.google.common.collect.Lists;

public class Iterable_KV_Test {
	public static void main(String[] args) throws IOException  
	{
		MutableDateTime mutableNow = Instant.now().toMutableDateTime();
		mutableNow.setDateTime(2017, 7, 12, 14, 0, 0, 0);
		Instant starttime = mutableNow.toInstant().plus(8*60*60*1000);
		int min;
		int sec;
		int millsec;
		min=2;
		sec=min*60;
		millsec=sec*1000;

		double[] value=new double[] {1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0};
			
		List<TimestampedValue<KV<String,KV<Integer, Double>>>> dataList = new ArrayList<>();
		int n=value.length;
		int count=0;
		for (int i=0; i<n; i++)
		{
			count=count+1;			
			if (i<=3)
			{
				Instant M1_time=starttime.plus(millsec*count);
				dataList.add(TimestampedValue.of(KV.of("M1", KV.of(i,value[i])), M1_time));	
			}				
			else if (4<=i && i<5)			
			{
				Instant M2_time=starttime.plus(millsec*count);
				dataList.add(TimestampedValue.of(KV.of("M2", KV.of(i,value[i])), M2_time));
			}
			else
			{
				Instant M3_time=starttime.plus(-millsec*count*2);				
				dataList.add(TimestampedValue.of(KV.of("M3", KV.of(i,value[i])), M3_time));
			}
		}
		
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());		
		PCollection<KV<String,KV<Integer, Double>>> data=p.apply("create data with time",Create.timestamped(dataList));		
		
		PCollection<KV<String, Iterable<KV<Integer, Double>>>> group_data=data
				.apply(GroupByKey.<String, KV<Integer, Double>>create());
		
		PCollection<KV<String, Iterable<KV<Integer, Double>>>> sort_group_data=group_data
				.apply(SortValues.<String,Integer,Double>create(BufferedExternalSorter.options()));
		
		PCollection<KV<String,KV<Integer, Double>>> processed_oneway_data=sort_group_data
				.apply(ParDo.
						of(new DoFn<KV<String, Iterable<KV<Integer, Double>>>, KV<String,KV<Integer, Double>>>()
						{
							@ProcessElement
							public void processElement(ProcessContext d) 
							{
								String MachineKey=d.element().getKey();
								Iterable<KV<Integer, Double>> DataElement=d.element().getValue();
								ArrayList<KV<Integer, Double>> ListDataElement=Lists.newArrayList(DataElement);
								int DataSize=Lists.newArrayList(ListDataElement).size();
								
								double real;
								for (int i = 0; i < DataSize; i++) 
								{
									real=ListDataElement.get(i).getValue();
									d.output(KV.of(MachineKey,KV.of(ListDataElement.get(i).getKey(),real)));
									
								}
							}
						}));
		/*
		PCollection<Void> print_processed_oneway_data=processed_oneway_data.apply(ParDo.
				of(new DoFn<KV<String,KV<Integer, Double>>,Void>(){
					@ProcessElement
					public void test(ProcessContext c){
						System.out.println(c.element());
					}
				}));
		*/
		p.run();
		
		
		
	}
}
