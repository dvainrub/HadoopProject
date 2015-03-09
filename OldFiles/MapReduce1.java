import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapReduce1 {


	public static class Map extends Mapper<TweetKey, TweetValue, Text, Text> {
		/*private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); 
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());  
        context.write(word, one);
      }
    }
  }*/
		private Text tweet = new Text();
		private String[] filteredWords;
		private Set<String> set;
		//private Writable[] arr;

		@Override
		public void map(TweetKey key, TweetValue value, Context context) throws IOException,  InterruptedException 
		{

//			if(Math.random() < 0.5){
				tweet = value.getTweetText();

				try{
					String filteredTweet = Filter.filterStopWords(tweet.toString());
					filteredWords = filteredTweet.split(" ");
					
					set = new TreeSet<String>();
					for (int i = 0; i < filteredWords.length; i++) 
					{
						set.add(filteredWords[i]);
					}
					
					String s = null;
					for (String uniqueWord : set) {
						/*arr = new Writable[2];
				arr[0] = new TweetKey(key);
				arr[1] = new TweetValue(value);*/
						
						s = key.toString() +TweetMapReduce.BETWEEN_KEY_AND_VALUE_DELIMETER+ value.toString();
						
						context.write(new Text(uniqueWord), new Text(s));
					}
				}catch(StringIndexOutOfBoundsException e){
					
				}

//			}

		}


	}


	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		//private Writable[] arr;
		//private MapWritable map;

		@Override
		public void reduce(Text word, Iterable<Text> tuples, Context context) throws IOException,  InterruptedException {

			int amountOfTweets=0;
			//map = new MapWritable();
			StringBuilder sb = new StringBuilder();
			for (Text tweetKeyValue : tuples) {
				//map.put(new IntWritable(amountOfTweets), tweetKeyValue);
				sb.append(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER + tweetKeyValue);
				amountOfTweets++;
			}

			/*arr = new Writable[2];
			arr[0] = new MapWritable(map);
			arr[1] = new IntWritable(amountOfTweets);*/

			sb.insert(0, amountOfTweets);

			//			long inputs = context.getCounter(Counters.gMAP_INPUT_RECORDS).getValue();
			//			String s = String.valueOf(inputs);
			//			
			//			context.write(new Text("Map Input Records"), new Text(s));

			context.write(word,new Text(sb.toString()));
		}
	}
	public static void main(String[] args) throws Exception {
		System.out.println("main of MapReduce1");

		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","2");
		//conf.set("mapred.reduce.tasks","2");

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "Create Word Dictionary");
		job1.setJarByClass(MapReduce1.class);
		job1.setMapperClass(MapReduce1.Map.class);
		job1.setReducerClass(MapReduce1.Reduce.class);

		//partitiomer
		//combiner
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TweetInputFormat.class);
		//    job.setCombinerClass(Reduce.class);
		//    job.setPartitionerClass(PartitionerClass.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		FileOutputFormat.setCompressOutput(job1, true);

		Counters counters = job1.getCounters();
		long mapInputRecordsCounterValue = counters.findCounter(Counter.MAP_INPUT_RECORDS).getValue();

		System.out.println("# of Input Records for Map1: "+String.valueOf(mapInputRecordsCounterValue));

		FileSystem fileSystem = FileSystem.get(URI.create(args[1]),conf);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(args[1]+TweetMapReduce.MAP_INPUT_RECORD_VALUE_S3_LOCATION));      
		//fsDataOutputStream.writeLong(mapInputRecordsCounterValue);
		PrintWriter writer  = new PrintWriter(fsDataOutputStream);
		writer.write(String.valueOf(mapInputRecordsCounterValue)); //"# of Input Records for Map1: "
		writer.close();
		fsDataOutputStream.close();  

		//TODO Tessting the merge
		//		String srcPath = "/user/hadoop/output"; 
		//		String dstPath = "/user/hadoop/merged_file"; 
		/*String srcPath = "s3n://default-ds-ass2-elidor/outputTwiter"; 
		String dstPath = "s3n://default-ds-ass2-elidor"; 
		Configuration conf2 = new Configuration(); 
		try { 
			FileSystem hdfs = FileSystem.get(conf2); 
			FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, conf2, null); 
			} 
		catch (IOException e) { }*/

	}
}
