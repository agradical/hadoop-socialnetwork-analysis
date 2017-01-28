import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriend {
	
	public static class Map extends Mapper<LongWritable, Text, Tuple, Text> {	
		private Text friendlist = new Text();
		
		Configuration conf = null;
		String userA = null;
		String userB = null;
		
		@Override
		public void setup(Context context) {
			conf = context.getConfiguration();
			userA = conf.get("userA");
			userB = conf.get("userB");
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyval = value.toString().split("\t");
			
			if(keyval.length < 2){
				return;
			}
			
			friendlist.set(keyval[1]);	
			String[] frndlst = keyval[1].split(",");
			if(userA != null && userB != null) {
				if(keyval[0].equals(userA) || keyval[0].equals(userB)) {
					for(int i=0; i<frndlst.length; i++) {
						Tuple tuple = new Tuple(keyval[0], frndlst[i]);
						context.write(tuple, friendlist);
					}
				}
			}
			else {
				for(int i=0; i<frndlst.length; i++) {
					Tuple tuple = new Tuple(keyval[0], frndlst[i]);
					context.write(tuple, friendlist);
				}		
			}
		}
	}
	
	public static class Reduce extends Reducer<Tuple, Text, Text, Text> {
		
		public void reduce(Tuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Set<String> friendset = new HashSet<String>();
			List<String> mutualfriend = new ArrayList<String>();
			Iterator<Text> iter = values.iterator();
			while(iter.hasNext()) {
				Text f = iter.next();
				String friendstr = f.toString();
				//System.out.println(friendstr);
				String[] friends = friendstr.split(",");
				for(int i=0; i<friends.length; i++) {
					if(friendset.contains(friends[i])) {
						mutualfriend.add(friends[i]);
					} else {
						friendset.add(friends[i]);
					}
				}
			}
			if(mutualfriend.size() > 0) {
				String[] frndlist = new String[mutualfriend.size()];
				frndlist = mutualfriend.toArray(frndlist);
				StringBuilder frndlist_str = new StringBuilder();
				if(frndlist.length > 0) {
					frndlist_str.append(frndlist[0]);
					for (int i=1; i<frndlist.length; i++) {
						frndlist_str.append(","+frndlist[i]);
					}
				}
				Text outkey = new Text(key.userA + "\t" + key.userB);
				Text outval = new Text(frndlist_str.toString());
				context.write(outkey, outval);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {		
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			if (otherArgs.length == 4) {
				conf.set("userA", otherArgs[2]);
				conf.set("userB", otherArgs[3]);
			} else {
				System.err.println("Usage: MutualFriend <in> <out> <userA> <userB>");
				System.exit(2);
			}
		}
		
		Job job = Job.getInstance(conf, "mutualfriend");
		
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
