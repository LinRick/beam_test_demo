package window_type_data;

import java.util.ArrayList;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
import org.joda.time.Instant;

public class Windows_Fourtypes_multi_keyvalue_SHOW{
	public static void main(String[] args) {	
		
		MutableDateTime mutableNow = Instant.now().toMutableDateTime();
		mutableNow.setDateTime(2017, 7, 12, 14, 0, 0, 0);
		Instant starttime = mutableNow.toInstant().plus(8*60*60*1000);//UTC+8
		//System.out.println(starttime);

		int min;
		int sec;
		int millsec;
		min=2;
		sec=min*60;
		millsec=sec*1000;

		
		List<TimestampedValue<KV<String,Integer>>> DataList = new ArrayList<>();		
		int[] value=new int[] {1,2,3,4,5,6,7,8,9,10};		
		int count=0;		
		for (int i=0; i<value.length; i++)
	    {
			count=count+1;			
			if (i<5)
			{
				Instant M1_time=starttime.plus(millsec*count);
				DataList.add(TimestampedValue.of(KV.of("M1", value[i]), M1_time));
			}
			else if (5<=i && i<8)			
			{
				Instant M2_time=starttime.plus(millsec*count);
				DataList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_time));
			}
			else
			{
				Instant M3_time=starttime.plus(-millsec*count*2);
				DataList.add(TimestampedValue.of(KV.of("M2", value[i]), M3_time));
			}
			//System.out.println(DataList.get(i));
		}		  

		//Create Pipeline (Direct Runner)
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);		

		/*
		//Use GlobalWindows		
		PCollection<KV<String, Integer>> DataPipe=p.apply("Input Data into Pipe",Create.timestamped(DataList))
				.apply(Window.<KV<String,Integer>>into(new GlobalWindows()))
				.apply(Sum.<String>integersPerKey());		

		//Print data
		PCollection<Void> PrintData=DataPipe.apply(ParDo.
				of(new DoFn<KV<String, Integer>,Void>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c){
				System.out.println(c.element());
			}
		}));
		
		//Run pipeline 
		p.run();
		*/


		//Use FixedWindows
		PCollection<KV<String, Integer>> DataPipe=p.apply("Input Data into Pipe",Create.timestamped(DataList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(2)))
						.discardingFiredPanes())
						.apply(Sum.<String>integersPerKey());				
		//Print data
		PCollection<Void> PrintData=DataPipe.apply(ParDo.
				of(new DoFn<KV<String, Integer>,Void>(){
			@ProcessElement
			public void ProcessElement(ProcessContext c){
				System.out.println(c.element());
			}
		}));

		//Print data		
		PCollection<String> DataPipeStr = DataPipe.apply("ToString Data",ToString.elements());		
		DataPipeStr.apply("write_to_txt",TextIO.write().to("/root/java_beam_test/output.txt"));		

		//Run pipeline 
		p.run();

		
		//FixedWindows.of/AfterWatermark.pastEndOfWindow
		/*
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))
						.triggering(AfterWatermark.pastEndOfWindow())
						.withAllowedLateness(Duration.standardMinutes(30))
						.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		*/
		
		//AfterProcessingTime.pastFirstElementInPane()		
		/*PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))
						.triggering(AfterProcessingTime.pastFirstElementInPane())
						.withAllowedLateness(Duration.standardMinutes(30))
						.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		*/
		
		
		
		//plusDelayOf (最大允許亂序時間)
		//withAllowedLateness (最大late時間)
		//AfterWatermark.pastEndOfWindow/withEarlyFirings
		
		/*count ok
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))		
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))						
								.triggering(AfterPane.elementCountAtLeast(2))
								.withAllowedLateness(Duration.standardMinutes(30))
								.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		*/
		
		

		

	}

	
	
	
}
	
