package mapReduceJobs;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tweet.MyMapWritable;
import tweet.StaticVars;
import tweet.Tweet;



public class MapReduce3 {

	/*
	Filter the stop words and iterate over the rest of the words in the tweet, 
	For every word in the tweet: Emit(word,TweetList)
	Same as Map1 but in this case we send a TweetList instead of a Tweet, containing: <Tweet, Norma, Vector>
	At the end of MapReduce3 we want to have all the tweets with at least 1 word in common grouped
	so we can calculate their cosine similarity.	
	 */	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		private String[] filteredWords;
		private Set<String> set;

		@Override
		public void map(LongWritable longKey, Text tabTweetNormaAndVector, Context context) throws IOException,  InterruptedException 
		{
			/*String tweetNormaAndVector = "\t[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:38 +0000 2015\",\"tweetId\":\"553523238525861890\"},"
					+ "\"tweetValue\":{\"text\":\"Having fun playing #CSRRacing for Android, why not join me for FREE?\\nhttp://t.co/h0EJsG8i4P  xxx\",\"retweeted\":false,\"favorited\":false,\"user\":\"nguyennam78\"}}, "
					+ "0.9801108198045045, "
					+ "[fun, 0.36201072644683135], [#CSRRacing, 0.36201072644683135], [, 0.20797801965573237], [playing, 0.36201072644683135], [join, 0.36201072644683135], [xxx, 0.36201072644683135], [Having, 0.36201072644683135], [Android,, 0.36201072644683135]]";
			 */

			try{


				String[] map3firstSplit = tabTweetNormaAndVector.toString().split("\t",2); 

				//All the info will be after the tab \t
				String tweetNormAndVector = map3firstSplit[1];

				//We take the substring(1) to remove the "[" at the beginning
				//Split in order to get in the first cell the whole Tweet
				String[] map3secondSplit = tweetNormAndVector.substring(1).split("\\}\\},");

				//The tweet is in the first cell but we add "}}" since we removed them in the split
				Tweet tweet = new Tweet(map3secondSplit[0]+"}}");

				String text = tweet.getTweetValue().getTweetText().toString();
				String filteredTweet = StaticVars.filterStopWords(text);
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
						System.out.println(uniqueWord);
						System.out.println(tabTweetNormaAndVector);
						context.write(new Text(uniqueWord), new Text(tweetNormAndVector));
					}
				}

			}
			catch (Exception e){

			}
		}
	}

	/*
	We receive a word and a TweetList of all the <Tweet,norma,word+tfidf pair> that conain that word
	For every word received, Emit( , Map[0, word
										 1, TweetList1
										 ...
										 n, TweetListn])
	Output for example: 
		String reduce3Output = "\t[#Android, "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:47 +0000 2015\",\"tweetId\":\"553523273720295425\"},\"tweetValue\":{\"text\":\"Criminal is under arrest! Fight the gangs in the streets of London! http://t.co/6vauasIpLq #Android #AndroidGames #GameInsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"קרן גלעדי\"}}, 0.9951903378201662, [#Android, 0.18718021769015913], [streets, 0.3258096538021482], [#GameInsight, 0.3258096538021482], [gangs, 0.3258096538021482], [arrest!, 0.3258096538021482], [Fight, 0.3258096538021482], [http://t.co/6vauasIpLq, 0.3258096538021482], [London!, 0.3258096538021482], [#AndroidGames, 0.3258096538021482], [Criminal, 0.3258096538021482]], "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523258931556352\"},\"tweetValue\":{\"text\":\"I've collected $2465! Think you can do better? http://t.co/BHKcJ5TawE #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"yana\"}}, 0.8637462516301732, [I've, 0.1466337068793427], [better?, 0.3258096538021482], [#Androidgames, 0.2564949357461537], [Think, 0.3258096538021482], [can, 0.3258096538021482], [#Gameinsight, 0.2564949357461537], [collected, 0.16486586255873817], [#Android, 0.18718021769015913], [$2465!, 0.3258096538021482], [http://t.co/BHKcJ5TawE, 0.3258096538021482]], "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523257471926272\"},\"tweetValue\":{\"text\":\"I finished the \\\"Shortage\\\" task in Big Business Deluxe for Android! http://t.co/aUC6ASoIxa #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"Igor Klašnja\"}}, 0.916258643925171, [Deluxe, 0.2961905943655893], [Big, 0.2961905943655893], [Android!, 0.2961905943655893], [finished, 0.2961905943655893], [#Android, 0.17016383426378104], [#Gameinsight, 0.23317721431468516], [http://t.co/aUC6ASoIxa, 0.2961905943655893], [task, 0.2961905943655893], [\"Shortage\", 0.2961905943655893], [Business, 0.2961905943655893], [#Androidgames, 0.23317721431468516]], "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:44 +0000 2015\",\"tweetId\":\"553523264040210432\"},\"tweetValue\":{\"text\":\"RT @3CX: Check out the new look for 3CXPhone for #Android - coming soon with #3CX Phone System 12.5 http://t.co/ws7Q1xyHY3 http://t.co/Eu0v\\u2026\",\"retweeted\":false,\"favorited\":false,\"user\":\"Mattias Kressmark\"}}, 0.7724339840392499, [#Android, 0.11698763605634946], [Phone, 0.20363103362634263], [coming, 0.16030933484134605], [look, 0.20363103362634263], [soon, 0.20363103362634263], [-, 0.20363103362634263], [12.5, 0.20363103362634263], [3CXPhone, 0.20363103362634263], [http://t.co/ws7Q1xyHY3, 0.20363103362634263], [new, 0.20363103362634263], [http://t.co/Eu0v…, 0.20363103362634263], [System, 0.20363103362634263], [RT, 0.13496776558458576], [Check, 0.20363103362634263], [#3CX, 0.20363103362634263], [@3CX:, 0.20363103362634263]]]";

	 */
	public static class Reduce extends Reducer<Text,Text,Text,MyMapWritable> {


		MyMapWritable map = new MyMapWritable();
		@Override
		public void reduce(Text word, Iterable<Text> tweets, Context context) throws IOException,  InterruptedException {
			try
			{//TODO throws IO Exception for lack of Java Heap Space
				int amountOfTweets=0;
				map = new MyMapWritable();
				for (Text text : tweets) 
				{

					map.put(new IntWritable(amountOfTweets+1), new Text(StaticVars.TWEET_LIST_DELIMITER+text));
					//in other words we make: map.put(key, List)
					amountOfTweets++;
				}

				map.put(new IntWritable(0), word);
				context.write(new Text(), map);
			} catch (Exception e) 
			{

			}
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
		job1.setOutputValueClass(MyMapWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);

		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
	}
}
