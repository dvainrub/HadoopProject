package mapReduceJobs;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import tweet.TweetValue;


public class MapReduce2 {

	/*
	From Reduce 1 we receive (word, amounts of Tweets in wich w appears, every tweet where w appears)
	In Map2 we extract every Tweet sent in the reducer.
	Calculate TFIDF this word
	Emit(Tweet(as a Text), Map(Word, TFIDF)
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, MyMapWritable> 
	{

		private Tweet tweet = new Tweet();
		private TweetValue value = new TweetValue();
		private MyMapWritable map = new MyMapWritable();

		
		@Override
		public void map(LongWritable  longKey, Text wordNumberOfTimesAndTweetsContainningIt, Context context) throws IOException,  InterruptedException 
		{
			int errorCounter = 0;
			try {
				if (wordNumberOfTimesAndTweetsContainningIt.toString().length() < 4) {
					/*The line is damaged (don't do anything), possible input:
					 [
					 \n\n  or \n
					 */
				}
				else{

					String[] map2firstSplit = wordNumberOfTimesAndTweetsContainningIt.toString().split("\t",2); 

					//The input text for the map is a line with a tab and then all the output from Reduce1
					//The information we need will be in the second cell of the array
					String mapReduce1CleanOutput;
					if (map2firstSplit.length!=2){
						/*This comes from a "damaged tweet, for example something like:
						[
						\n\n   or  \n
						#Android, .....]
						And we received only "#Android, .....]", so we add "[" at the beginning to proceed
						 */
						mapReduce1CleanOutput = "["+map2firstSplit[0];
					}
					else {
						//not damaged input
						mapReduce1CleanOutput = map2firstSplit[1];
					}

					//The output from Reduce1 is of the form: (word, amounts of Tweets in wich w appears, every tweet where w appears)
					//We separate the first word from the rest with ",\space" in two cells:
					String[] map2secondSplit = mapReduce1CleanOutput.split(", ", 2);

					//We take a substring starting from index 1 from the first cell because it start with an empty space
					String word = map2secondSplit[0].substring(1);

					//The second cell contains the number and tweet, separated by a comma "," therefore we split:

					String [] map2thirdSplit = map2secondSplit[1].split(",",2);
					double amountOfTweetsThatContainTheWord = Double.valueOf(map2thirdSplit[0]);

					//Total Amount of Tweets in the file
					long totalAmountOfTweets = context.getConfiguration().getLong("mapInputRecordsCounterValue", 100000000);
					//TODO cambiar a esto-->	long totalAmountOfTweets = context.getConfiguration().getLong("mapInputRecordsCounterValue", 0);

					//IDF is the same for every tweet
					double idf = calculateIDF(totalAmountOfTweets, amountOfTweetsThatContainTheWord);

					//Third cell are all the tweets containing the word, each one starts with "tweetKey:"
					String[] allTweetsContainningTheWord = map2thirdSplit[1].split("\\}\\},");


					double tf, tf_idf;
					errorCounter = 0;
					for (int i = 0; i < allTweetsContainningTheWord.length; i++) 
					{
						//We removed "}}" in the split so we add them here again
						tweet = new Tweet (allTweetsContainningTheWord[i]+"}}");
						value = tweet.getTweetValue();
						map = new MyMapWritable();

						tf = value.calc_tf(word);
						tf_idf = tf * idf;

						map.put(new IntWritable(0), new Text(word));
						map.put(new IntWritable(1), new DoubleWritable(tf_idf));
						context.write(new Text(tweet.toString()), map);

					}
				}
			}
			catch(Exception e)
			{
				String uri = "s3://"+StaticVars.S3_PROJECT_BUCKET_NAME+"/";
				String path = "s3://"+StaticVars.S3_PROJECT_BUCKET_NAME+"/badFiles/badFile"+errorCounter;
				String text = "Tweet at errorCounter="+errorCounter+":\n "+ wordNumberOfTimesAndTweetsContainningIt+"\nError: "+e.getMessage() +"\ntweet: "+ tweet + "\n\n";
				StaticVars.writeToS3(uri, context.getConfiguration(),path, text );

				errorCounter++;
			}
		}

		//tf(t, d) = LOG(#all tweets/total num of d that contain t)
		//t = term, d = tweet,
		private  double calculateIDF(long totalAmountOfTweets, double amountOfTweetsThatContainTheWord) {
			return Math.log(totalAmountOfTweets/amountOfTweetsThatContainTheWord);
		}

	}


	/*
	In Reduce2 we receive a Tweet and an Iterable conaining all the words of the tweet and their respective TFIDF
	The output will be the Tweet with a list that contains every word in the tweets + its TFIDF
	Emit(Tweet, Norm of the Tweet, Map(Map(word,TFIDF)))

	Output for example: 
	String reduce2Output = "[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:38 +0000 2015\",\"tweetId\":\"553523238525861890\"},"
				+ "\"tweetValue\":{\"text\":\"Having fun playing #CSRRacing for Android, why not join me for FREE?\\nhttp://t.co/h0EJsG8i4P  xxx\",\"retweeted\":false,\"favorited\":false,\"user\":\"nguyennam78\"}}, "
				+ "0.9801108198045045, "
				+ "[fun, 0.36201072644683135], [#CSRRacing, 0.36201072644683135], [, 0.20797801965573237], [playing, 0.36201072644683135], [join, 0.36201072644683135], [xxx, 0.36201072644683135], [Having, 0.36201072644683135], [Android,, 0.36201072644683135]]";
	 */
	public static class Reduce extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {
		private MyMapWritable map = new MyMapWritable();

		public void reduce(Text tweet, Iterable<MyMapWritable> valueAndWordTfIdf, Context context) throws IOException,  InterruptedException {
			int i = 0;
			List<DoubleWritable> list = new ArrayList<DoubleWritable>();

			map = new MyMapWritable();
			for (MyMapWritable myMapWritable : valueAndWordTfIdf) {
				map.put(new IntWritable(i+2), new Text(myMapWritable.toString()));
				list.add((DoubleWritable) myMapWritable.get(new IntWritable(1))); //get the TFIDF value of the word
				i++;
			}

			map.put(new IntWritable(0), tweet);
			map.put(new IntWritable(1), new DoubleWritable(norm(list)));
			context.write(new Text(), map);
		}

		//calculate the norma of A = sqrt(tfidf_word_0^2 + tfidf_word_1^2 + ... + tfidf_word_n^2)
		public double norm(List<DoubleWritable> listOfTFIDF) {
			double sum = 0;
			for (DoubleWritable tf_idf_word_i : listOfTFIDF) {
				sum += Math.pow(tf_idf_word_i.get(), 2);
			}
			return Math.sqrt(sum);
		}
	}


	public static void main(String[] args) throws Exception {
		System.out.println("main of :"+MapReduce2.class);
		System.out.println();

		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","2");
		//conf.set("mapred.reduce.tasks","2");

		FileSystem fileSystem = FileSystem.get(URI.create(args[0]),conf);
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path(args[0]+StaticVars.MAP_INPUT_RECORD_VALUE_S3_LOCATION));      
		BufferedReader bf = new BufferedReader(new InputStreamReader(fsDataInputStream));
		String str = bf.readLine();
		bf.close();
		fsDataInputStream.close();  

		System.out.println("BufferedLine= "+str);
		long mapInputRecordsCounterValue = Long.valueOf(str);
		conf.setLong("mapInputRecordsCounterValue", mapInputRecordsCounterValue);

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "MapReduce2");
		job1.setJarByClass(MapReduce2.class);
		job1.setMapperClass(MapReduce2.Map.class);
		job1.setReducerClass(MapReduce2.Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MyMapWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MyMapWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);

		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));


		job1.waitForCompletion(true);

		System.out.println("# of Input Records for Map2: "+String.valueOf(mapInputRecordsCounterValue));



	}
}
