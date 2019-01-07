package window_type_data;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;

public class FixWindow_ParDo_Print_WindowTime {

	
	private static class PrintingDoFn extends DoFn<KV<String,Integer>, String> {
	    @ProcessElement
	    public void processElement(ProcessContext c, BoundedWindow window) {
	      c.output(c.element() + ":" + c.timestamp().getMillis()
	          + ":" + window.maxTimestamp().getMillis());
	      
	      System.out.println(c.element() + "¡CEvent time:" + c.timestamp()	 
	           + "¡CMinWindow time:" + window.TIMESTAMP_MIN_VALUE        
	           + "¡CMaxWindow time:" + window.maxTimestamp());
	      
	      //System.out.println(timestamp.getMillis());
	      
	      //PaneInfo pane = c.pane();
	      //System.out.println(c.element() + " | " + pane.getIndex()); 
	           
	      
	      //System.out.println(c.pane());
	      //System.out.println(c.pane().getIndex());
	      
	      //System.out.println(window.TIMESTAMP_MAX_VALUE);
	      //System.out.println(window.TIMESTAMP_MIN_VALUE);
	      

	    }
	  }
	
	public static void main(String[] args) {
		MutableDateTime mutableNow = Instant.now().toMutableDateTime();
		mutableNow.setDateTime(2017, 7, 12, 14, 0, 0, 0);
		Instant starttime = mutableNow.toInstant().plus(8*60*60*1000);//UTC+8		
		
		int min;
		int sec;
		int millsec;
		min=2;
		sec=min*60;
		millsec=sec*1000;	
		
		List<TimestampedValue<KV<String,Integer>>> stringList = new ArrayList<TimestampedValue<KV<String, Integer>>>();		
		int[] value=new int[] {1,2,3,4,5,6,7,8,9,10};		
		int count=0;		
		for (int i=0; i<value.length; i++)
	    {
			count=count+1;			
			if (i==0)
			{
				Instant M1_time_test=starttime.plus(millsec*count);
				Instant timestamp=M1_time_test;
				long start = timestamp.getMillis() - timestamp.plus(Duration.standardMinutes(7))
						.minus(Duration.standardMinutes(0)).
						getMillis() % Duration.standardMinutes(7).getMillis();
				
				System.out.println(timestamp);
				System.out.println(new Instant(start));
				System.out.println(new Instant(start).plus(Duration.standardMinutes(7)));
			}
			
			if (i<5)
			{
				Instant M1_time=starttime.plus(millsec*count);
				stringList.add(TimestampedValue.of(KV.of("M1", value[i]), M1_time));

			}
			else if (5<=i && i<8)			
			{
				Instant M2_time=starttime.plus(millsec*count);
				//stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_now));
				stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_time));
			}
			else
			{
				Instant M3_time=starttime.plus(3*millsec*count);
				//stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M2_now));
				stringList.add(TimestampedValue.of(KV.of("M2", value[i]), M3_time));
			}
			//System.out.println(stringList.get(i));
			
			//System.out.println(i+1);
			//System.out.println(60*(i+1)%7);
		}
		
		
		//starttime
		
		
		
		
		
		
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);		
		/*
		PCollection<KV<String,Integer>> testdata=p.apply("createsideinput",Create.timestamped(stringList))
				.apply(Window.<KV<String,Integer>>into(FixedWindows
						.of(Duration.standardMinutes(5)))		
						.discardingFiredPanes())
				.apply(Sum.<String>integersPerKey());
		
		PCollection<KV<String, Integer>> b=testdata.apply(ParDo
				.of(new DoFn<KV<String, Integer>,KV<String, Integer>>(){
			@ProcessElement
			//@Override
			public void test(ProcessContext c){				
				c.output(c.element());
				System.out.println(c.element());
			}
		}));
		*/

		//FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(5));
		//IntervalWindow window = windowFn.assignWindow(new Instant(10));
		//System.out.println(new Instant(10)); //1970-01-01T00:00:00.010Z
		//System.out.println(new Instant(0)); //1970-01-01T00:00:00.000Z
		
		/*
		PCollection<KV<String,Integer>> testdata=p.apply("createsideinput",
				Create.timestamped(stringList))
		
				.apply(Window.<KV<String,Integer>>configure()					
						.into(FixedWindows
								.of(Duration.standardMinutes(13)))
						
						);
		*/
		PCollection<KV<String,Integer>> testdata=p.apply("createsideinput",
				Create.timestamped(stringList))		
				.apply(Window.<KV<String,Integer>>into(FixedWindows.of(Duration.standardMinutes(7)).withOffset(Duration.ZERO)));
				
		PCollection<String> b=testdata.apply(ParDo.of(new PrintingDoFn()));
		
		
		p.run();
	}

}
