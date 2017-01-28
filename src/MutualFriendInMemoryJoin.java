import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriendInMemoryJoin {
	
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
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] keyval = value.toString().split("\t");
			
			if(keyval.length < 2){
				return;
			}
			friendlist.set(keyval[1]);

			if(userA != null && userB != null) {
				if(conf.get("userA").equals(keyval[0]) || conf.get("userB").equals(keyval[0])) {
					Tuple tuple = new Tuple(conf.get("userA"), conf.get("userB"));
					context.write(tuple, friendlist);
				}
			} else {
				String[] frndlst = keyval[1].split(",");
				for(int i=0; i<frndlst.length; i++) {
					Tuple tuple = new Tuple(keyval[0], frndlst[i]);
					context.write(tuple, friendlist);
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Tuple, Text, Text, Text> {
		
		HashMap<String, String> users = new HashMap<String, String>();
		
		URI[] uri;
		@Override
		public void setup (Context context) throws IOException {
			uri = DistributedCache.getCacheFiles(context.getConfiguration());
			try{
				BufferedReader readBuffer1 = new BufferedReader(new FileReader(uri[0].toString()));
                String line;
                while ((line=readBuffer1.readLine())!=null){
                    String[] userInfo = line.split(",");
                	users.put(userInfo[0], userInfo[1]+":"+userInfo[9]);
                }
                readBuffer1.close(); 
            }       
            catch (Exception e){
                System.out.println(e.toString());
            }
		}
		
		public void reduce(Tuple key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			Iterator<Text> iter = values.iterator();
			Set<String> friendset = new HashSet<String>();
			List<String> mutualfriend = new ArrayList<String>();
			
			while(iter.hasNext()) {
				Text f = iter.next();
				String friendstr = f.toString();
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
					frndlist_str.append("["+users.get(frndlist[0]));
					for (int i=1; i<frndlist.length; i++) {
						frndlist_str.append(","+users.get(frndlist[i]));
					}
					frndlist_str.append("]");
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

		if (otherArgs.length != 3) {
			if (otherArgs.length == 5) {
				conf.set("userA", otherArgs[3]);
				conf.set("userB", otherArgs[4]);
			} else {
				System.err.println("Usage: MutualFriendInMemoryJoin <filetocache> <in> <out> <userA> <userB>");
				System.exit(2);
			}
		}
		
		DistributedCache.addCacheFile(new URI(otherArgs[0]), conf);//(conf, otherArgs[0]);
		
		Job job = Job.getInstance(conf, "mutualfriend");//new Job(conf, "mutualfriend");
		
		job.setJarByClass(MutualFriendInMemoryJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//job.setCombinerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(Text.class);
		
		Path inpath = new Path(otherArgs[1]);
		Path outpath  = new Path(otherArgs[2]);
		System.out.println(inpath.toUri().toString());
		System.out.println(outpath.toUri().toString());
		
		FileInputFormat.addInputPath(job, inpath);
		FileOutputFormat.setOutputPath(job, outpath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
