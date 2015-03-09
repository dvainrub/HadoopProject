import java.io.IOException;
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


public class NEWMapReduce4 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{


		@Override
		public void map(LongWritable longKey, Text tweetsKeyAndValAndTfIdfVecAndNormContainTheWord, Context context) throws IOException,  InterruptedException 
		{

			//			try{
			String[] valueAndKey = tweetsKeyAndValAndTfIdfVecAndNormContainTheWord.toString().split("\t", 2);

			String[] tweets = valueAndKey[1].split(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER);
			
			String aTdIdfVec;
			String bTfIdfVec;
			String tweetJ;
			String tweetAndCosineSimilarity = null;
			TreeSet<String> similarityTweets = new TreeSet<>();
			int numOfTweets = context.getConfiguration().getInt("nParameter", 4);
			boolean newTweet = false;
			
			for(int i = 1; i < tweets.length; i++){
				for(int j = 1; j < tweets.length; j++){
					if(i != j){
						//if(tweets[j].split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER).length > 1){
							aTdIdfVec = tweets[i].split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[1];
							bTfIdfVec = tweets[j].split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[1];
							tweetJ = tweets[j];
							tweetAndCosineSimilarity = calcCosineSimilarity(aTdIdfVec, bTfIdfVec) + TweetMapReduce.BETWEEN_COSINESIMILARITY_AND_TWEET_DELIMETER + tweetJ;
							newTweet = true;
							similarityTweets.add(tweetAndCosineSimilarity);
							if(similarityTweets.size()>numOfTweets){
								similarityTweets.pollFirst();
							}
						//}
					}
					if(newTweet){
						newTweet = false;
						while (!similarityTweets.isEmpty()) {
							context.write(new Text(tweets[i]), new Text(similarityTweets.pollFirst()));
						}
					}
				}
			}
			//			}catch (Exception e){
			//				System.out.println(e.toString());
			//			}
		}


		//calculate the multiplying A by B
		protected double multAB(String a, String b) {
			double sum = 0;
			a = a.replaceAll(TweetMapReduce.BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER, TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER);
			b = b.replaceAll(TweetMapReduce.BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER, TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER);

			String[] aVec = a.split(TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER);
			String[] bVec = b.split(TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER);
			double tfIdfA = 0;
			double tfIdfB = 0;
			for (int indexA = 1; indexA < aVec.length; indexA++) {
				indexA++;
				for (int indexB = 1; indexB < bVec.length; indexB++) {
					if(aVec[indexA-1].equals(bVec[indexB])){
						indexB++;
						tfIdfA = Double.valueOf(aVec[indexA]);
						tfIdfB = Double.valueOf(bVec[indexB]);
						sum += tfIdfA*tfIdfB;
					}else{
						indexB++;
					}
				}
			}
			return sum;
		}

		// multingAB/(normA*normB)
		protected double calcCosineSimilarity(String aTfIdfVecAndNorm, String bTfIdfVecAndNorm) {
			String[] aVecAndNorm = aTfIdfVecAndNorm.split(TweetMapReduce.BETWEEN_TFIDF_VEC_AND_NORMA_DELIMETER);
			String[] bVecAndNorm = bTfIdfVecAndNorm.split(TweetMapReduce.BETWEEN_TFIDF_VEC_AND_NORMA_DELIMETER);
			//			System.out.println(aVecAndNorm[0]);
			//			System.out.println(aVecAndNorm[1] + "\n");
			double normA = Double.valueOf(aVecAndNorm[1]);
			double normB = Double.valueOf(bVecAndNorm[1]);
			//			System.out.println(normA + "\n");
			//			System.out.println(normB + "\n");
			double multingAB = multAB(aVecAndNorm[0], bVecAndNorm[0]);
			//			System.out.println(multingAB + "\n");
			double cosineSimilarity = (multingAB/(normA*normB));
			return cosineSimilarity;
		}




	}


	public static class Reduce extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text tweet, Iterable<Text> tweetsAndCosineSimilarity, Context context) throws IOException,  InterruptedException {

			StringBuilder sb = new StringBuilder();
			TreeSet<String> similarityTweets = new TreeSet<>();
			for (Text oneTweetAndCosineSimilarity : tweetsAndCosineSimilarity) {
				similarityTweets.add(oneTweetAndCosineSimilarity.toString());
			}
			String simTweet = similarityTweets.pollLast();
			int numOfTweets = context.getConfiguration().getInt("nParameter", 4);
			for (int i = 1; (i <= numOfTweets) && (simTweet != null) ; i++ ){

				//				sb.append(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER + simTweet);
				//				simTweet = simTweet.split(TweetMapReduce.BETWEEN_COSINESIMILARITY_AND_TWEET_DELIMETER)[1];
				simTweet = simTweet.split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[0];
				sb.append("\n\t" + simTweet);
				simTweet = similarityTweets.pollLast();

				//				sb.append(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER + simTweet);
				//				simTweet = similarityTweets.pollLast();
			}

			//			String tweetShow = tweet.toString().split(TweetMapReduce.BETWEEN_COSINESIMILARITY_AND_TWEET_DELIMETER)[1];
			String tweetShow = tweet.toString().split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[0];
			//			context.write(tweet,new Text(sb.toString()));
			context.write(new Text(tweetShow),new Text(sb.toString()));


			//			context.write(tweet,new Text(sb.toString()));
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
		job1.setJarByClass(NEWMapReduce4.class);
		job1.setMapperClass(NEWMapReduce4.Map.class);
		job1.setReducerClass(NEWMapReduce4.Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);

		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
//		FileOutputFormat.setCompressOutput(job1, true);


		System.out.println("Integer.valueOf(args[2]): " + Integer.valueOf(args[2]));
		job1.waitForCompletion(true);

		Counters counters = job1.getCounters();
		long mapInputRecordsCounterValue = counters.findCounter(Counter.MAP_INPUT_RECORDS).getValue();

		System.out.println("# of Input Records for Map3: "+String.valueOf(mapInputRecordsCounterValue));

	}
}
