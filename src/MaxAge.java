import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MaxAge {
	
	public static class Map_One_One extends Mapper<LongWritable, Text, Text, Text> {	
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split(",");
			Text text = new Text(val[3]+"::"+val[9]);
			Text outkey = new Text("R::"+val[0]);
			context.write(outkey, text);
		}
	}
	
	public static class Map_One_Two extends Mapper<LongWritable, Text, Text, Text> {	
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split("\t");
			if(val.length > 1) {
				String[] friendlist = val[1].split(",");
				for (int i=0; i<friendlist.length; i++) {
					Text text = new Text(val[0]);
					Text outkey = new Text("S::"+friendlist[i]);
					context.write(outkey, text);
				}
			}
		}
	}
	
	public static class Reduce_One extends Reducer<Text, Text, Text, Text> {
		
		Map<String, String> usermap =  new HashMap<String, String>();
		Map<String, Integer> maxage =  new HashMap<String, Integer>();
		
		SimpleDateFormat dateformat = new SimpleDateFormat("mm/dd/yyyy");
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iter  = values.iterator();
			while(iter.hasNext()) {
				Text value = iter.next();
				if(key.toString().contains("R::")) {
					usermap.put(key.toString().substring(3), value.toString());
				} else if (key.toString().contains("S::")){
					String entrykey = key.toString().substring(3);
					if(usermap.containsKey(entrykey)) {
						String addr_dob_str = usermap.get(entrykey);
						String[] addr_dob = addr_dob_str.split("::");
						Date date = new Date();
						try {
							date = dateformat.parse(addr_dob[1]);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						Calendar cal = Calendar.getInstance();
						cal.setTime(date);
						int birthyear = cal.get(Calendar.YEAR);

						Date currdate = new Date();
						cal.setTime(currdate);
						int curryear = cal.get(Calendar.YEAR);

						int age = curryear - birthyear;
						if(maxage.containsKey(value.toString())) {
							if(age > maxage.get(value.toString())) {
								maxage.put(value.toString(), age);
								context.write(value, new Text(age+"::"+addr_dob[0]));
							}
						} else {
							maxage.put(value.toString(), age);
							context.write(value, new Text(age+"::"+addr_dob[0]));
						}
					}
				}
			}
		}
	}

	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {	
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split("\t");
			Text outkey = new Text(val[0]);
			Text text = new Text(val[1]);
			context.write(outkey, text);
		}
	}	
		
	public static class Reduce_Two extends Reducer<Text, Text, IntWritable, Text> {
		

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Iterator<Text> iter = values.iterator();
			int max_age = 0;
			String addr = "";
			while(iter.hasNext()) {
				Text value = iter.next();
				String[] vals = value.toString().split("::");
				if(max_age < Integer.parseInt(vals[0])) {
					max_age = Integer.parseInt(vals[0]);
					addr = vals[1];
				}
			}
			context.write(new IntWritable(max_age), new Text(key.toString()+"::"+addr));
		}
	}

	public static class Map_Three extends Mapper<LongWritable, Text, Text, Text> {	

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] val = value.toString().split("\t");
			Text outkey = new Text(val[0]);
			Text text = new Text(val[1]);
			context.write(outkey, text);
		}
	}

	public static class Reduce_Three extends Reducer<Text, Text, Text, Text> {
		int i=0;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(i<10) {
				Iterator<Text> iter = values.iterator();
				Text value = new Text();
				while(iter.hasNext()) {
					if(i<10) {
						value = iter.next();
						String[] vals = value.toString().split("::");
						context.write(new Text(vals[0]), new Text(vals[1]+", "+key.toString()));
						i++;
					} else {
						break;
					}
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: MaxAge <in1> <in2> <out>");
			System.exit(2);
		}
				
		Job job_one = Job.getInstance(conf, "friendsage");		
		job_one.setJarByClass(MaxAge.class);
		job_one.setReducerClass(Reduce_One.class);
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(Text.class);
		job_one.setMapOutputKeyClass(Text.class);
		job_one.setMapOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job_one, new Path(otherArgs[0]), TextInputFormat.class, Map_One_One.class);
		MultipleInputs.addInputPath(job_one, new Path(otherArgs[1]), TextInputFormat.class, Map_One_Two.class);
		FileOutputFormat.setOutputPath(job_one, new Path("first_reducer_output"));
		
		job_one.waitForCompletion(true);		
		
		Job job_two = Job.getInstance(conf, "friendmaxage");
		job_two.setJarByClass(MaxAge.class);
		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);
		job_two.setOutputKeyClass(IntWritable.class);
		job_two.setOutputValueClass(Text.class);
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		job_two.setSortComparatorClass(CustomComparator.class);

		FileInputFormat.addInputPath(job_two, new Path("first_reducer_output"));
		FileOutputFormat.setOutputPath(job_two, new Path("second_reducer_output"));

		job_two.waitForCompletion(true);
		
		Job job_three = Job.getInstance(conf, "sort");
		job_three.setJarByClass(MaxAge.class);
		job_three.setMapperClass(Map_Three.class);
		job_three.setReducerClass(Reduce_Three.class);
		job_three.setOutputKeyClass(Text.class);
		job_three.setOutputValueClass(Text.class);
		job_three.setMapOutputKeyClass(Text.class);
		job_three.setMapOutputValueClass(Text.class);
		job_three.setSortComparatorClass(CustomComparator.class);

		FileInputFormat.addInputPath(job_three, new Path("second_reducer_output"));
		FileOutputFormat.setOutputPath(job_three, new Path(otherArgs[2]));
		
		System.exit(job_three.waitForCompletion(true) ? 0 : 1);

	}
}
