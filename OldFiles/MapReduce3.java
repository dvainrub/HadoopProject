import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapReduce3 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		
		
		@Override
		public void map(LongWritable longKey, Text valueAndTfIdfVector, Context context) throws IOException,  InterruptedException 
		{
			
			String[] valueAndKey = valueAndTfIdfVector.toString().split("\t", 2);
			String tweetKey = valueAndKey[0];
			String valAndKey = valueAndKey[1];
			
			String[] s = valAndKey.split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER);
			
			TweetValue tweetValue = new TweetValue(s[0]);
			String tweet = tweetValue.getTweetText().toString();
			String filteredTweet = Filter.filterStopWords(tweet);
			String[] filteredWords = filteredTweet.split(" ");
			
			Set<String> set = new TreeSet<String>();
			for (int i = 0; i < filteredWords.length; i++) 
			{
				set.add(filteredWords[i]);
			}
			
			String str = null;
			for (String uniqueWord : set) {
				str = tweetKey +TweetMapReduce.BETWEEN_KEY_AND_VALUE_DELIMETER+ valAndKey;
				context.write(new Text(uniqueWord), new Text(str));
			}
		}

	}

	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text word, Iterable<Text> tuples, Context context) throws IOException,  InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text tweetKeyValue : tuples) {
				//TODO remember that the first one is ##karish##...
				sb.append(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER + tweetKeyValue);
			}
			context.write(word,new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("main of MapReduce3");
		
		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","2");
		//conf.set("mapred.reduce.tasks","2");

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "MapReduce3");
		job1.setJarByClass(MapReduce3.class);
		job1.setMapperClass(MapReduce3.Map.class);
		job1.setReducerClass(MapReduce3.Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setCompressOutput(job1, true);
		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		Counters counters = job1.getCounters();
		long mapInputRecordsCounterValue = counters.findCounter(Counter.MAP_INPUT_RECORDS).getValue();

		System.out.println("# of Input Records for Map3: "+String.valueOf(mapInputRecordsCounterValue));
		


	}
}
