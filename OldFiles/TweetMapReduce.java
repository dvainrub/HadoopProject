//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.Reducer;

@SuppressWarnings("deprecation")
public class TweetMapReduce {
	public final static String TWEET_OR_VALUE_FIELDS_DELIMETER = "##karish##";
	public final static String BETWEEN_KEY_AND_VALUE_DELIMETER = "##tiburon##";
	public final static String BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER = "##shark##";
	public final static String BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER = "##nena##";
	public static final String BETWEEN_WORD_AND_TFIDF_DELIMETER = "##eliyahuRon##";
	public static final String BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER = "##yosefdissapeard##";
	public static final String SLASH_N_DELIMETER = "##SlashNDelimeter##";
	public static final String BETWEEN_TFIDF_VEC_AND_NORMA_DELIMETER = "##huecoG##";
	public static final String BETWEEN_COSINESIMILARITY_AND_TWEET_DELIMETER = "##tairninja##";
	public static int numOfTweets = 2;
	public static final String MAP_INPUT_RECORD_VALUE_S3_LOCATION = "//mapInputRecordValue.txt";
	public static final String TAB_DELIMITER = "##TAB_DELIMITER##";




	/*org.apache.hadoop.mapreduce.Counters counters = job1.getCounters();
		long counter = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();*/

	//System.exit(job1.waitForCompletion(true) ? 0 : 1);


	/*Configuration conf1 = new Configuration();

		Job job1 = new Job(conf1, "Create Word Dictionary");
		job1.setJarByClass(TweetMapReduce.class);
		job1.setMapperClass(MapReduce1.Map.class);
		job1.setReducerClass(MapReduce1.Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TweetInputFormat.class);


		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("intermediate"));




		Configuration conf2 = new Configuration();

		Job job2 = new Job(conf2, "Create Word Dictionary");
		job2.setJarByClass(TweetMapReduce.class);
		job2.setMapperClass(MapReduce2.Map.class);
		job2.setReducerClass(MapReduce2.Reduce.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TweetInputFormat.class);


		FileInputFormat.addInputPath(job2, new Path("intermediate"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.addDependingJob(job1);


		JobControl jc = new JobControl("JC");
		jc.addJob(job1);
		jc.addJob(job2);
		jc.run();*/

	/*JobConf jobconf1 = new JobConf(new Configuration());

		@SuppressWarnings("deprecation")
		Job job1 = new Job(jobconf1);
		jobconf1.setJarByClass(TweetMapReduce.class);
		jobconf1.setMapperClass(MapReduce1.Map.class);
		jobconf1.setReducerClass(MapReduce1.Reduce.class);

		jobconf1.setMapOutputKeyClass(Text.class);
		jobconf1.setMapOutputValueClass(Text.class);

		jobconf1.setOutputKeyClass(Text.class);
		jobconf1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1.getJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job1.getJob(), new Path("intermediate"));

		JobConf jobconf2 = new JobConf(new Configuration());
		@SuppressWarnings("deprecation")
		Job job2 = new Job(jobconf2);
		jobconf2.setJarByClass(TweetMapReduce.class);
		jobconf2.setMapperClass(MapReduce2.Map.class);
		jobconf2.setReducerClass(MapReduce2.Reduce.class);

		jobconf2.setMapOutputKeyClass(Text.class);
		jobconf2.setMapOutputValueClass(Text.class);

		jobconf2.setOutputKeyClass(Text.class);
		jobconf2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1.getJob(), new Path("intermediate"));
		FileOutputFormat.setOutputPath(job1.getJob(), new Path(args[1]));

		job2.addDependingJob(job1);


		JobControl jc = new JobControl("JC");
		jc.addJob(job1);
		jc.addJob(job2);
		jc.run();
	 */


}










