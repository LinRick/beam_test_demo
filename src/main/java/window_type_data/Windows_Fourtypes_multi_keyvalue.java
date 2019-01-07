package window_type_data;



import java.util.ArrayList;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;

import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;

import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
import org.joda.time.Instant;

public class Windows_Fourtypes_multi_keyvalue {
	public static void main(String[] args) {	
		
		MutableDateTime mutableNow = Instant.now().toMutableDateTime();

		int min;
		int sec;
		int millsec;
		min=2;
		sec=min*60;
		millsec=sec*1000;

		System.out.println(mutableNow);

		mutableNow.setMillisOfSecond(0);
		mutableNow.setSecondOfMinute(0);
		Instant now = mutableNow.toInstant().plus(8*60*60*1000); //UTC+8		
		//System.out.println(now);

		List<TimestampedValue<KV<String,Integer>>> stringList = new ArrayList<>();
		
		int[] value=new int[] {1,2,3,4,5,6,7,8,9,10};		
		
		int count=0;
		
		for (int i=0; i<value.length; i++)
	    {
			count=count+1;			
			if (i<5)
			{
				Instant M1_now=now.plus(millsec*count);
				stringList.add(TimestampedValue.of(KV.of("M1", value[i]), M1_now));
			}
			else if (5<=i && i<8)			
			{
				Instant M2_now=now.plus(millsec*count);
				//stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_now));
				stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_now));
			}
			else
			{
				Instant M3_now=now.plus(millsec*count*10);
				//stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_now));
				stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M3_now));
			}
			System.out.println(stringList.get(i));
		}		  
		
	
		PipelineOptions options = PipelineOptionsFactory.create();		
		
		Pipeline p = Pipeline.create(options);		
		
		//GlobalWindows
		/*
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped
				(stringList))
				.apply(Window.<KV<String,Integer>>into(new GlobalWindows()))
				.apply(Sum.<String>integersPerKey());		
		*/
		
		//FixedWindows.of (窗口時間)
		//plusDelayOf (最大允許亂序時間)
		//withAllowedLateness (最大late時間)
		//AfterWatermark.pastEndOfWindow

		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))
						.triggering(AfterWatermark.pastEndOfWindow())
						.withAllowedLateness(Duration.standardMinutes(30))
						.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());


		//AfterWatermark.pastEndOfWindow/withEarlyFirings
		/*
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))
						.triggering(AfterWatermark.pastEndOfWindow()
								.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
										.plusDelayOf(Duration.standardMinutes(2))))
						.withAllowedLateness(Duration.standardMinutes(30))
						.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		*/
		
		
		
		//AfterProcessingTime.pastFirstElementInPane()
		/*
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))
						.triggering(AfterProcessingTime.pastFirstElementInPane())
						.withAllowedLateness(Duration.standardMinutes(30))
						.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		*/
		
		
		/*count ok
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))		
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(5)))						
								.triggering(AfterPane.elementCountAtLeast(2))
								.withAllowedLateness(Duration.standardMinutes(30))
								.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		*/
		
		
		PCollection<String> sum_str = testdata.apply("int_to_str data",ToString.elements());		

		sum_str.apply("write_to_txt",TextIO.write().to("/root/java_beam_test/sumint.txt"));
	
		p.run();
		
		

	}

	
	
	
}
	
