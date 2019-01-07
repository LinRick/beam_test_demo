package window_type_data;


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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.MutableDateTime;
import org.joda.time.Instant;

public class Windows_Fourtypes_keyvalue {
	public static void main(String[] args) {	
		
		MutableDateTime mutableNow = Instant.now().toMutableDateTime();
		
		long a =mutableNow.toDate().getTime();
		long b =mutableNow.toDate().getTime()-100;
		
		System.out.println(a);
		System.out.println(b);
		mutableNow.setMillisOfSecond(0);
		
		//System.out.println(mutableNow.toDate().getTime());
		
		
		Instant now = mutableNow.toInstant();
		
		
		
		
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
		
		/*
		PipelineOptions options = PipelineOptionsFactory.create();		
		Pipeline p = Pipeline.create(options);	
		
		
		//GlobalWindows
		
		PCollection<KV<String, Integer>> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(KV.of("M1", 1),now),TimestampedValue.of(KV.of("M1", 2),now1),
				TimestampedValue.of(KV.of("M2", 5),now),TimestampedValue.of(KV.of("M2", 5),now)))
				.apply(Window.<KV<String,Integer>>into(new GlobalWindows()))
				.apply(Sum.<String>integersPerKey());
		
		
		//FixedWindows
		/*
		PCollection<Integer> testdata=p.apply("createsideinput",Create.timestamped(
				TimestampedValue.of(1,now),TimestampedValue.of(2,now1),TimestampedValue.of(5,now)))
				.apply(Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(2))))
				.apply(Sum.integersGlobally().withoutDefaults());
		*/

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
		
		/*		
		PCollection<String> sum_str = testdata.apply("int_to_str data",ToString.elements());		

		sum_str.apply("write_to_txt",TextIO.write().to("/root/java_beam_test/sumint.txt"));
	
		p.run();
		*/
		
	}
	
	
	
}
	