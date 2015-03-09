package mapReduceJobs;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tweet.MyMapWritable;
import tweet.StaticVars;
import tweet.Tweet;
import tweet.TweetInputFormat;
import tweet.TweetKey;
import tweet.TweetValue;



@SuppressWarnings("deprecation")
public class MapReduce1 {

	/*
	Filter the stop words and iterate over the rest of the words in the tweet, 
	For every word in the tweet: Emit(word,Tweet)
	 */
	public static class Map extends Mapper<TweetKey, TweetValue, Text, Tweet> {

		private Text tweet = new Text();
		private String[] filteredWords;
		private Set<String> set;


		@Override
		public void map(TweetKey key, TweetValue value, Context context) throws IOException,  InterruptedException 
		{
			tweet = value.getTweetText();

			String filteredTweet = StaticVars.filterStopWords(tweet.toString());
			filteredWords = filteredTweet.split(" ");

			//We add the filtered words to a set to remove duplicates
			set = new TreeSet<String>();
			for (int i = 0; i < filteredWords.length; i++) 
			{
				set.add(filteredWords[i]);
			}

			for (String uniqueWord : set) {
				if(!StaticVars.problematicWord(uniqueWord))
				{
					context.write(new Text(uniqueWord), new Tweet(new TweetKey(key),new TweetValue(value)));
				}
			}
		}
	}

	/*
	For every word received, Emit(word, amounts of Tweets in wich w appears, every tweet where w appears)
	Output for example: 

	String reduce1Output = "\t[#Android, 4,"
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:47 +0000 2015\",\"tweetId\":\"553523273720295425\"},"
				+ "\"tweetValue\":{\"text\":\"Criminal is under arrest! Fight the gangs in the streets of London! http://t.co/6vauasIpLq #Android #AndroidGames #GameInsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"קרן גלעדי\"}},"
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523258931556352\"},"
				+ "\"tweetValue\":{\"text\":\"I've collected $2465! Think you can do better? http://t.co/BHKcJ5TawE #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"yana\"}}, "
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:44 +0000 2015\",\"tweetId\":\"553523264040210432\"},"
				+ "\"tweetValue\":{\"text\":\"RT @3CX: Check out the new look for 3CXPhone for #Android - coming soon with #3CX Phone System 12.5 http://t.co/ws7Q1xyHY3 http://t.co/Eu0v\\u2026\",\"retweeted\":false,\"favorited\":false,\"user\":\"Mattias Kressmark\"}},"
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523257471926272\"},"
				+ "\"tweetValue\":{\"text\":\"I finished the \\\"Shortage\\\" task in Big Business Deluxe for Android! http://t.co/aUC6ASoIxa #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"Igor Klašnja\"}}]";
	 */
	public static class Reduce extends Reducer<Text,Tweet,Text,MyMapWritable> {

		MyMapWritable map = new MyMapWritable();

		@Override
		public void reduce(Text word, Iterable<Tweet> tweets, Context context) throws IOException,  InterruptedException {

			int amountOfTweets=0;
			map = new MyMapWritable();
			for (Tweet tweet : tweets) 
			{

				map.put(new IntWritable(amountOfTweets+2), new Text(tweet.toString()));
				//in other words we make: map.put(key, Tweet(key,value))
				amountOfTweets++;
			}

			map.put(new IntWritable(0), word);
			map.put(new IntWritable(1), new IntWritable(amountOfTweets));

			context.write(new Text(), map);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("main of MapReduce1");

		Configuration conf = new Configuration();

		Job job1 = new Job(conf, "Create Word Dictionary");
		job1.setJarByClass(MapReduce1.class);
		job1.setMapperClass(MapReduce1.Map.class);
		job1.setReducerClass(MapReduce1.Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Tweet.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MyMapWritable.class);

		job1.setInputFormatClass(TweetInputFormat.class);

		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		TweetInputFormat.addInputPath(job1, new Path(args[0]));//<--Add this
		//REMOVE THIS-->FileInputFormat.addInputPath(job1, new Path(args[0]));


		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		Counters counters = job1.getCounters();
		long mapInputRecordsCounterValue = counters.findCounter(Counter.MAP_INPUT_RECORDS).getValue();

		System.out.println("# of Input Records for Map1: "+String.valueOf(mapInputRecordsCounterValue));

		/*Write the number of Input Records for the first map to a bucket in S3*/
		String path = args[1]+StaticVars.MAP_INPUT_RECORD_VALUE_S3_LOCATION;
		String text = String.valueOf(mapInputRecordsCounterValue);
		StaticVars.writeToS3(args[1], conf, path, text);
	}
}
