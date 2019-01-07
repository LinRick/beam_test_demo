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


public class Windows_Fourtypes {
	public static void main(String[] args) throws IOException {	
		
		/*
		String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format(new Date());
		Date now1 = new Date();
		System.out.println(timeStamp);
		System.out.println(now1);		
		Instant t = null;
		t = t.now();
		System.out.println(t);
		*/	
		
		MutableDateTime mutableNow = org.joda.time.Instant.now().toMutableDateTime();
		mutableNow.setMillisOfSecond(0);
		org.joda.time.Instant now = mutableNow.toInstant();

		
		mutableNow.setDateTime(2017, 7, 6, 13, 40, 0, 0);
		org.joda.time.Instant now1 = mutableNow.toInstant();
		
		
		System.out.println(now);
		System.out.println(now1);
		
		//String timeStamp = new SimpleDateFormat("HH.mm.ss").format(new Date());
		/*
		System.out.println(timeStamp);
		Date dt=sdf.parse(str);

		Calendar rightNow = Calendar.getInstance();
		rightNow.setTime(date);
		
		rightNow.add(rightNow.HOUR_OF_DAY,-10);
		Date dt1=rightNow.getTime();
		String reStr = rightNow.format(dt1);
		System.out.println(reStr);
		*/
		
		
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);	

		//GlobalWindows
		/*
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))
				.apply(Window.<Integer>into(new GlobalWindows()))
				.apply(Sum.integersGlobally().withoutDefaults());
		*/	
		
		//FixedWindows
		
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))
				.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(2))))
				.apply(Sum.integersGlobally().withoutDefaults());
		

		//SlidingWindows
		/*
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))
				.apply(Window.<Integer>into(SlidingWindows.of(Duration.standardMinutes(2))))
				.apply(Sum.integersGlobally().withoutDefaults());
		*/

		// Sessions
		/*
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
			TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))
			.apply(Window.<Integer>into(Sessions.withGapDuration(Duration.standardMinutes(1))))
			.apply(Sum.integersGlobally().withoutDefaults());
		*/
		
		
		PCollection<String> sum_str = testdata.apply("int_to_str data",ToString.elements());		

		sum_str.apply("write_to_txt",TextIO.write().to("/root/java_beam_test/sumint.txt"));
	
		p.run();
		
		
	}
	
	
	
}
	