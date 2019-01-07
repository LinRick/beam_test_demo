package window_type_data;

import java.util.Arrays;
import java.util.Calendar;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.List;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
//import java.time.Instant;


public class Windows_Trigger_Fourtypes {
	public static void main(String[] args) throws IOException {	
		
				
		MutableDateTime mutableNow = org.joda.time.Instant.now().toMutableDateTime();
		mutableNow.setMillisOfSecond(0);
		org.joda.time.Instant now = mutableNow.toInstant();
		
		mutableNow.setDateTime(2017, 7, 6, 13, 40, 0, 0);
		org.joda.time.Instant now1 = mutableNow.toInstant();
		
		
		System.out.println(now);
		System.out.println(now1);
			
		
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);	
		//Trigger:Count/Time/Both
		
		//FixedWindows/Trigger:Count
		/*
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))		
				.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(2)))						
								.triggering(AfterPane.elementCountAtLeast(2))
								.withAllowedLateness(Duration.standardMinutes(30))
								.discardingFiredPanes())								
				.apply(Sum.integersGlobally().withoutDefaults());
		*/
		
		//FixedWindows/Trigger:Time:Trigger Time:AfterWatermark(event time)
		//plusDelayOf:允許亂序的時間:lag(hour/min/sec delay)
		//withAllowedLateness:多久的時間後變會停止執行 trigger
		//accumulatingFiredPanes(): 當窗口被觸發後進行accumulation(累積)並且"不會"刪除原本窗口中的內容
		//discardingFiredPanes: 當窗口被觸發後進行action並且刪除原本窗口中的內容
		
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))		
				
				.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(2)))						
								.triggering(AfterWatermark.pastEndOfWindow()
										.withLateFirings(AfterProcessingTime.pastFirstElementInPane()
												.plusDelayOf(Duration.standardMinutes(1))))								
								.withAllowedLateness(Duration.standardMinutes(30))								
								.discardingFiredPanes())				
				.apply(Sum.integersGlobally().withoutDefaults());		
		
				
		//FixedWindows/Trigger:Time:AfterProcessingTime(processing time)
		/*PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
			TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))		
		
			.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(2)))						
						.triggering(AfterProcessingTime.pastFirstElementInPane()
                        	.plusDelayOf(Duration.standardMinutes(1)))						
						.withAllowedLateness(Duration.standardMinutes(30))						
						.discardingFiredPanes()		
			
			.apply(Sum.integersGlobally().withoutDefaults());		
		*/
		
		
		PCollection<String> sum_str = testdata.apply("int_to_str data",ToString.elements());		

		sum_str.apply("write_to_txt",TextIO.write().to("/root/java_beam_test/sumint.txt"));
	
		p.run();
		
		
	}
	
	
	
}
	