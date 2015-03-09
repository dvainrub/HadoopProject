import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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


public class MapReduce2 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{


		@Override
		public void map(LongWritable  longKey, Text mapOfTuplesAndNumberOfTweetsThatContainTheWord, Context context) throws IOException,  InterruptedException 
		{

			String[] valueAndKey = mapOfTuplesAndNumberOfTweetsThatContainTheWord.toString().split("\t", 2);
			String word = valueAndKey[0];
			if(valueAndKey.length==2){
				String[] tuplesOfTweets = valueAndKey[1].split(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER);
				double amountOfTweetsThatContainTheWord = Integer.valueOf(tuplesOfTweets[0]);

				//TODO--CALCULATE TOTAL AMOUT OF TWEETS WITH COUNTER
				long mapInputRecordsCounterValue = context.getConfiguration().getLong("mapInputRecordsCounterValue", 100000000);
				double idf = calc_idf(mapInputRecordsCounterValue, amountOfTweetsThatContainTheWord); //same for every tweet

				TweetKey[] key = new TweetKey[tuplesOfTweets.length-1];
				TweetValue[] value = new TweetValue[tuplesOfTweets.length-1];

				String[] keyAndValue;
				String mapKey, mapValue;
				int errorCounter =0;
				for (int i = 0; i < key.length ; i++)
				{
					keyAndValue = tuplesOfTweets[i+1].split(TweetMapReduce.BETWEEN_KEY_AND_VALUE_DELIMETER);
					try {
						
						key[i] = new TweetKey(keyAndValue[0]);
						value[i] = new TweetValue(keyAndValue[1]);
						double tf = value[i].calc_tf(word);
						double tf_idf = tf * idf;

						mapKey = key[i].toString();
						mapValue = value[i].toString()+TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER+word+TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER+tf_idf;

						context.write(new Text(mapKey), new Text(mapValue));
					} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
						
						FileSystem fileSystem = FileSystem.get(URI.create("s3://default-ds-ass2-elidor/"),context.getConfiguration());
						FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://default-ds-ass2-elidor/badFiles/badFile"+errorCounter));      
						PrintWriter writer  = new PrintWriter(fsDataOutputStream);
						writer.write("keyAndValue at errorCounter="+errorCounter+": "+tuplesOfTweets[i+1]+"\n\tkeyAndValue[0]: "+keyAndValue[0]+"\n\n");
						writer.close();
						fsDataOutputStream.close();  
						errorCounter++;
					}
					
				}
			}
		}


		//tf(t, d) = LOG(#all tweets/total num of d that contain t)
		//t = term, d = tweet,
		private double calc_idf(long mapInputRecordsCounterValue, double amountOfTweetsThatContainTheWord) {
			return Math.log(mapInputRecordsCounterValue/amountOfTweetsThatContainTheWord);
		}

	}


	public static class Reduce extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text tweetKey, Iterable<Text> valueAndWordTfIdf, Context context) throws IOException,  InterruptedException {

			StringBuilder sb = new StringBuilder();
			String[] s = null;
			for (Text tweetValueAndWordTfIdf : valueAndWordTfIdf) {
				s = tweetValueAndWordTfIdf.toString().split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER);
				sb.append(TweetMapReduce.BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER + s[1]);
			}
			sb.append(TweetMapReduce.BETWEEN_TFIDF_VEC_AND_NORMA_DELIMETER + normaA(sb.toString()));
			sb.insert(0, s[0] + TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER);
			context.write(tweetKey, new Text(sb.toString()));
		}

		//calculate the norma of A
		public static double normaA(String a) {
			double sum = 0;
			String[] aVec = a.split(TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER);

			double tfIdf = 0;
			for (int indexA = 1; indexA < aVec.length; indexA++) {
				tfIdf = Double.valueOf(aVec[indexA].split(TweetMapReduce.BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER)[0]);
				sum += Math.pow(tfIdf, 2);
			}
			return Math.sqrt(sum);
		}



	}


	public static void main(String[] args) throws Exception {
		System.out.println("main of MapReduce2");




		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","2");
		//conf.set("mapred.reduce.tasks","2");

		FileSystem fileSystem = FileSystem.get(URI.create(args[0]),conf);
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path(args[0]+TweetMapReduce.MAP_INPUT_RECORD_VALUE_S3_LOCATION));      
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

		System.out.println("# of Input Records for Map2: "+String.valueOf(mapInputRecordsCounterValue));



	}
}
