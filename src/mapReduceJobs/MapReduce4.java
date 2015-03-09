package mapReduceJobs;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
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


public class MapReduce4 {

	/*
	From Reduce 3 we receive Map[0, word
								1, TweetList1
								...
								n, TweetListn]
	In Map4 we extract every TweetList and build a Data Structure for easy access to all the elements (Tweet, Norma and Vector)
	Then calculate the cosine similarity between each of them
	Emit(TweetA, Map(CosineSimilarity, TweetB))
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, MyMapWritable> 
	{
		MyMapWritable collectionOfTweetsMap = new MyMapWritable();
		MyMapWritable tweetMap =  new MyMapWritable();
		MyMapWritable vectorMap =  new MyMapWritable();
		MyMapWritable tweetMapA =  new MyMapWritable();
		MyMapWritable tweetMapB =  new MyMapWritable();
		MyMapWritable tweetAndCosSim =  new MyMapWritable();

		@Override
		public void map(LongWritable longKey, Text tweetsKeyAndValAndTfIdfVecAndNormContainTheWord, Context context) throws IOException,  InterruptedException 
		{
			/*String reduce3Output = "\t[#Android, "
					+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:47 +0000 2015\",\"tweetId\":\"553523273720295425\"},\"tweetValue\":{\"text\":\"Criminal is under arrest! Fight the gangs in the streets of London! http://t.co/6vauasIpLq #Android #AndroidGames #GameInsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"קרן גלעדי\"}}, 0.9951903378201662, [#Android, 0.18718021769015913], [streets, 0.3258096538021482], [#GameInsight, 0.3258096538021482], [gangs, 0.3258096538021482], [arrest!, 0.3258096538021482], [Fight, 0.3258096538021482], [http://t.co/6vauasIpLq, 0.3258096538021482], [London!, 0.3258096538021482], [#AndroidGames, 0.3258096538021482], [Criminal, 0.3258096538021482]], "
					+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523258931556352\"},\"tweetValue\":{\"text\":\"I've collected $2465! Think you can do better? http://t.co/BHKcJ5TawE #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"yana\"}}, 0.8637462516301732, [I've, 0.1466337068793427], [better?, 0.3258096538021482], [#Androidgames, 0.2564949357461537], [Think, 0.3258096538021482], [can, 0.3258096538021482], [#Gameinsight, 0.2564949357461537], [collected, 0.16486586255873817], [#Android, 0.18718021769015913], [$2465!, 0.3258096538021482], [http://t.co/BHKcJ5TawE, 0.3258096538021482]], "
					+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523257471926272\"},\"tweetValue\":{\"text\":\"I finished the \\\"Shortage\\\" task in Big Business Deluxe for Android! http://t.co/aUC6ASoIxa #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"Igor Klašnja\"}}, 0.916258643925171, [Deluxe, 0.2961905943655893], [Big, 0.2961905943655893], [Android!, 0.2961905943655893], [finished, 0.2961905943655893], [#Android, 0.17016383426378104], [#Gameinsight, 0.23317721431468516], [http://t.co/aUC6ASoIxa, 0.2961905943655893], [task, 0.2961905943655893], [\"Shortage\", 0.2961905943655893], [Business, 0.2961905943655893], [#Androidgames, 0.23317721431468516]], "
					+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:44 +0000 2015\",\"tweetId\":\"553523264040210432\"},\"tweetValue\":{\"text\":\"RT @3CX: Check out the new look for 3CXPhone for #Android - coming soon with #3CX Phone System 12.5 http://t.co/ws7Q1xyHY3 http://t.co/Eu0v\\u2026\",\"retweeted\":false,\"favorited\":false,\"user\":\"Mattias Kressmark\"}}, 0.7724339840392499, [#Android, 0.11698763605634946], [Phone, 0.20363103362634263], [coming, 0.16030933484134605], [look, 0.20363103362634263], [soon, 0.20363103362634263], [-, 0.20363103362634263], [12.5, 0.20363103362634263], [3CXPhone, 0.20363103362634263], [http://t.co/ws7Q1xyHY3, 0.20363103362634263], [new, 0.20363103362634263], [http://t.co/Eu0v…, 0.20363103362634263], [System, 0.20363103362634263], [RT, 0.13496776558458576], [Check, 0.20363103362634263], [#3CX, 0.20363103362634263], [@3CX:, 0.20363103362634263]]]";
			 */
			if (tweetsKeyAndValAndTfIdfVecAndNormContainTheWord.toString().length() < 60) {
				/*The line is damaged (don't do anything), possible input:
				 [
				 \n\n  or \n
				 */
			}
			else{


				String [] map4firstSplit = tweetsKeyAndValAndTfIdfVecAndNormContainTheWord.toString().split("\t",2);

				String mapReduce4CleanOutput;
				if (map4firstSplit.length!=2){
					/*This comes from a "damaged tweet, for example something like:
				[
				\n\n   or  \n
				#Android, .....]
				And we received only "#Android, .....]", so we add "[" at the beginning to proceed
					 */
					mapReduce4CleanOutput = "["+map4firstSplit[0];
				}
				else {
					//not damaged input
					mapReduce4CleanOutput = map4firstSplit[1];
				}

				//The output from Reduce3 is of the form: (word, TweetList, TweetList,...,TweetList)
				//We separate the first word from the rest with ",\space" in two cells:
				String[] map4secondSplit = mapReduce4CleanOutput.split(", ", 2);


				String [] map4thirdSplit = map4secondSplit[1].split(StaticVars.TWEET_LIST_DELIMITER);


				convertToMapDataStructure(map4thirdSplit, collectionOfTweetsMap, tweetMap, vectorMap);

				for(int i = 0; i < collectionOfTweetsMap.length(); i++){
					for(int j = 0; j < collectionOfTweetsMap.length(); j++){
						if(i != j)
						{
							tweetMapA = (MyMapWritable) collectionOfTweetsMap.get(new IntWritable(i));
							tweetMapB = (MyMapWritable) collectionOfTweetsMap.get(new IntWritable(j));

							double cosineSimilarity = calcCosineSimilarity(tweetMapA, tweetMapB);

							if (cosineSimilarity<0.9999 && cosineSimilarity>0) 
							{
								Tweet tweetA = (Tweet) tweetMapA.get(new IntWritable(0));
								Tweet tweetB = (Tweet) tweetMapB.get(new IntWritable(0));

								tweetAndCosSim = new MyMapWritable();
								tweetAndCosSim.put(new IntWritable(0), new DoubleWritable(cosineSimilarity));
								tweetAndCosSim.put(new IntWritable(1), tweetB);
								context.write(new Text(tweetA.toString()), tweetAndCosSim);
							}
						}
					}
				}
			}
		}

		/*
		 * Given an input from reduce3, convert it to a MapWritable Data Structure of the form:
		 * collectionOfTweetsMap[0, tweetMap1,
		 * 						...
		 * 						 n, tweetMapN]
		 * 
		 * tweetMap[0, Tweet,
		 * 			1, Norma,
		 * 			2, vectorMap]
		 * 
		 * vectorMap[0, word, 1, tfidf,
		 * 			 2, word, 3, tfidf,
		 * 			 ...
		 * 			 2n, word, 2n+1, tfidf]
		 */
		public static void convertToMapDataStructure(String[] map4thirdSplit,
				MyMapWritable collectionOfTweets, MyMapWritable tweetMap, MyMapWritable vectorMap) {
			int collectionNumber = 0;
			for (String tweetList : map4thirdSplit) //TweetList = [Tweet, norma, vector]
			{
				if (tweetList.length()<5)
				{
					//corrupted line, dont do anything
				}
				else
				{
					tweetMap = new MyMapWritable();
					String [] map4forthSplit = tweetList.split("\\}\\}, ");

					//We removed "}}" in the split so we add it here to keep the json format
					String tweetString = map4forthSplit[0]+"}}";
					//Substring to eliminate the "[" at the beginning
					Tweet tweet = new Tweet(tweetString.substring(1));
					tweetMap.put(new IntWritable(0), tweet);
					//Example: [{"tweetKey":{"createdAt":"Fri Jan 09 12:06:47 +0000 2015","tweetId":"553523273720295425"},
					//		"tweetValue":{"text":"Criminal is under arrest! Fight the gangs in the streets of London! http://t.co/6vauasIpLq #Android #AndroidGames #GameInsight","retweeted":false,"favorited":false,"user":"קרן גלעדי"}}

					String[] map4fifthSplit = map4forthSplit[1].split(",",2);
					double norma = Double.valueOf(map4fifthSplit[0]);
					tweetMap.put(new IntWritable(1), new DoubleWritable(norma));
					//Example: 0.9951903378201662

					//substring to eliminate the last "]]"
					//split to separate each word-tfidf pair
					String[] map4sixthSplit = map4fifthSplit[1].substring(0, map4fifthSplit[1].length()-2).split("]");

					String cleanPair;
					vectorMap = new MyMapWritable();
					int wordCounter = 0;
					for (String wordTfidfPair : map4sixthSplit) {
						//We remove things like space or ',' at the beginning of the pair
						cleanPair = wordTfidfPair.substring(2);
						if (cleanPair.substring(0,1).equals("[")) //If the pair starts with "[" we remove it
						{
							cleanPair = cleanPair.substring(1);
						}
						String[] wordTfidfPairs = cleanPair.split(", 0.");
						if (wordTfidfPairs.length!=2) {
							//problematic word, dont do anything
						}
						else
						{
							String word = wordTfidfPairs[0];
							double tf_idf = Double.valueOf("0."+wordTfidfPairs[1]);
							vectorMap.put(new IntWritable(wordCounter), new Text(word));
							vectorMap.put(new IntWritable(wordCounter+1), new DoubleWritable(tf_idf));
							wordCounter+=2;
						}
					}
					tweetMap.put(new IntWritable(2), vectorMap);
					//					System.out.println("TweetMap: "+tweetMap);
					collectionOfTweets.put(new IntWritable(collectionNumber), tweetMap);
					collectionNumber++;
				}
			}
		}



		// A*B/(normA*normB)
		public static double calcCosineSimilarity(MyMapWritable tweetMapA, MyMapWritable tweetMapB) {
			double multiplicationValue = multiplyABVectors(tweetMapA, tweetMapB);
			double normA = ((DoubleWritable) tweetMapA.get(new IntWritable(1))).get();
			double normB = ((DoubleWritable) tweetMapB.get(new IntWritable(1))).get();

			double cosineSimilarity = (multiplicationValue/(normA*normB));
			return cosineSimilarity;
		}
	}
	
	protected static double multiplyABVectors(MyMapWritable tweetMapA, MyMapWritable tweetMapB){
		MyMapWritable vectorMapA = (MyMapWritable) tweetMapA.get(new IntWritable(2));
		MyMapWritable vectorMapB = (MyMapWritable) tweetMapB.get(new IntWritable(2));

		double sum = 0;
		double tfIdfA = 0;
		double tfIdfB = 0;
		for (int indexA = 0; indexA < vectorMapA.length(); indexA++) 
		{
			String wordA = ((Text)vectorMapA.get(new IntWritable(indexA))).toString();
			indexA++;
			for (int indexB = 0; indexB < vectorMapB.length(); indexB++) 
			{
				String wordB = ((Text)vectorMapB.get(new IntWritable(indexB))).toString();
				indexB++;
				if (wordA.equals(wordB))
				{
					tfIdfA = ((DoubleWritable)vectorMapA.get(new IntWritable(indexA))).get();
					tfIdfB = ((DoubleWritable)vectorMapB.get(new IntWritable(indexB))).get();
					sum += tfIdfA*tfIdfB;
				}
			}
		}

		return sum;
	}

	/*
	We received an argument N as the third argument,
	Reduce4 will print every Tweet and a list of the TopN similar tweets (by cosine similarity)
	 */
	public static class Reduce extends Reducer<Text,MyMapWritable,Text,Text> {

		@Override
		public void reduce(Text tweet, Iterable<MyMapWritable> tweetsAndCosineSimilarity, Context context) throws IOException,  InterruptedException {

			TreeSet<String> similarityTweets = new TreeSet<>();
			for (MyMapWritable oneTweetAndCosineSimilarity : tweetsAndCosineSimilarity) {
				similarityTweets.add(oneTweetAndCosineSimilarity.toString());
			}

			int numOfSimilarTweets = context.getConfiguration().getInt("nParameter", 4); //TODO: Change to 0
			String similarTweet = similarityTweets.pollLast();
			StringBuilder sb = new StringBuilder();

			for (int i = 0; (i < numOfSimilarTweets) && (similarTweet != null) ; i++ )
			{
				//TODO print prettier	similarTweet = similarTweet.split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[0];
				sb.append("\n\t" + similarTweet);
				similarTweet = similarityTweets.pollLast();

			}

			context.write(new Text(tweet),new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("main of MapReduce4");

		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","2");
		//conf.set("mapred.reduce.tasks","2");
		conf.setInt("nParameter", Integer.valueOf(args[2]));

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "MapReduce4");
		job1.setJarByClass(MapReduce4.class);
		job1.setMapperClass(MapReduce4.Map.class);
		job1.setReducerClass(MapReduce4.Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MyMapWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);

		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));


		System.out.println("Integer.valueOf(args[2]): " + Integer.valueOf(args[2]));
		job1.waitForCompletion(true);
	}
}
