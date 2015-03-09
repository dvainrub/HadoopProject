package tweet;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class StaticVars {

	//****************************DELIMITERS
	public static final String SLASH_N_DELIMETER = "##SlashNDelimeter##";
	public static final String TWEET_LIST_DELIMITER = "##TWEET_LIST##";
	public static final String TAB_DELIMITER = "##TAB_DELIMITER##";


	//****************************EC2
	public static final String EC2_INSTANCE_KEY_NAME = "first_instance_ds";

	//****************************S3
	public static final String MAP_INPUT_RECORD_VALUE_S3_LOCATION = "//mapInputRecordValue.txt";
	public static final String S3_PROJECT_BUCKET_NAME = "dsps151-ass2-elidor";
	public static final String S3_BIG_CORPUS = "s3n://dsp151/tweetCorpus.txt";
	public static final String S3_MEDIUM_CORPUS = "s3n://dsp151/smallCorpus";
	public static final String S3_SMALL_CORPUS = "s3n://dsp151/TinnyCorpus.txt";
	public static final String S3_INTERMIDIATE = "s3n://"+S3_PROJECT_BUCKET_NAME+"/Intermediate";

	public static final String S3_DEFAULT_INPUT_NAME = "/29Tweets.txt";
	public static final String S3_DEFAULT_INPUT = "s3n://"+S3_PROJECT_BUCKET_NAME+S3_DEFAULT_INPUT_NAME;
	public static final String S3_LOG = "s3n://"+S3_PROJECT_BUCKET_NAME+"/logs/";

	//****************************METHODS
	public static void writeToS3(String uri, Configuration conf, String path, String text) throws IOException{
		FileSystem fileSystem = FileSystem.get(URI.create(uri),conf);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path));      
		PrintWriter writer  = new PrintWriter(fsDataOutputStream);
		writer.write(text);
		writer.close();
		fsDataOutputStream.close(); 
	}

	public static boolean problematicWord(String uniqueWord) {
		return uniqueWord.contains("FREE?") || 
				uniqueWord.contains("sports") ||
				uniqueWord.contains("android") ||
				uniqueWord.contains("health") ||
				uniqueWord.contains("iphone") ||
				uniqueWord.contains("food") ||
				uniqueWord.contains("Charlie") ||
				uniqueWord.contains("fit") ;
	}

	//***********************FILTER
	public static String filterStopWords(String text) {
		int length = text.length();
		//for each stop word - delete is from the tweet text
		text = " " + text + " ";
		for (String word : STOP_WORDS) {
			text = text.replaceAll(" " + word + " ", " ");
			text = text.replaceAll(" " + word.toUpperCase() + " ", " ");
			while (length != text.length()) {
				length = text.length();
				text = text.replaceAll(" " + word + " ", " ");
				text = text.replaceAll(" " + word.toUpperCase() + " ", " ");
			}
		}
		return text.substring(1, (text.length()-1));
	}

	static final String[] STOP_WORDS = {"a","about","above","after","again","against","all","am","an","and","any","are","aren\'t","as","at","be",
		"because","been","before","being","below","between","both","but","by","can\'t","cannot","could",
		"couldn\'t","did","didn\'t","do","does","doesn\'t","doing","don\'t","down","during","each","few","for","from",
		"further","had","hadn\'t","has","hasn\'t","have","haven\'t","having","he","he\'d","he\'ll","he\'s",
		"her","here","here\'s","hers","herself","him","himself","his","how","how\'s","i","i\'d","i\'ll","i\'m",
		"i\'ve","if","in","into","is","isn\'t","it","it\'s","its","itself","let\'s","me","more","most","mustn\'t",
		"my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves",
		"out","over","own","same","shan\'t","she","she\'d","she\'ll","she\'s","should","shouldn\'t","so","some","such",
		"than","that","that\'s","the","their","theirs","them","themselves","then","there","there\'s","these","they",
		"they\'d","they\'ll","they\'re","they\'ve","this","those","through","to","too","under","until","up","very","was",
		"wasn\'t","we","we\'d","we\'ll","we\'re","we\'ve","were","weren\'t","what","what\'s","when","when\'s","where",
		"where\'s","which","while","who","who\'s","whom","why","why\'s","with","won\'t","would","wouldn\'t","you","you\'d",
		"you\'ll","you\'re","you\'ve","your","yours","yourself","yourselves", 
//		"fit", "sports", "android", "health", "iphone", "movie", "food", "Charlie",
		"|"};


	//***************EMR Configurations
	public static enum InputSize { Big, Medium, Small, Default }

	public static void configHadoopJarSteps(
			ArrayList<StepConfig> stepsConfigs, InputSize inputSize, String argsForJob4, int amountOfSteps) {

		int i = 1;
		//----------------------STEP 1-------------------------------
		String input = "s3n://"+S3_PROJECT_BUCKET_NAME+S3_DEFAULT_INPUT;
		if (inputSize==InputSize.Big)
		{
			input = S3_BIG_CORPUS;
			System.out.println(inputSize+" configuration");
		}
		else if(inputSize==InputSize.Medium)
		{
			input =S3_MEDIUM_CORPUS;
			System.out.println(inputSize+" configuration");
		}
		else if(inputSize==InputSize.Small)
		{
			input =S3_SMALL_CORPUS;
			System.out.println(inputSize+" configuration");
		}
		
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		.withJar("s3://"+S3_PROJECT_BUCKET_NAME+"/MapReduce1.jar") // This should be a full map reduce application.
		.withArgs(input, S3_INTERMIDIATE+i+inputSize);

		StepConfig stepConfig1 = new StepConfig()
		.withName("MapReduce"+i+inputSize)
		.withHadoopJarStep(hadoopJarStep1)
		.withActionOnFailure("TERMINATE_JOB_FLOW");
		System.out.println("Running Step: "+i);
		i++;
		stepsConfigs.add(stepConfig1);

		//----------------------STEP 2-------------------------------
		if(i<=amountOfSteps)
		{
			HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()

			.withJar("s3://"+S3_PROJECT_BUCKET_NAME+"/MapReduce2.jar") // This should be a full map reduce application.
			.withArgs(S3_INTERMIDIATE+(i-1)+inputSize,S3_INTERMIDIATE+i+inputSize);


			StepConfig stepConfig2 = new StepConfig()
			.withName("MapReduce"+i+inputSize)
			.withHadoopJarStep(hadoopJarStep2)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
			System.out.println("Running Step: "+i);
			i++;
			stepsConfigs.add(stepConfig2);
		}

		//----------------------STEP 3-------------------------------
		if(i<=amountOfSteps)
		{
			HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
			.withJar("s3://"+S3_PROJECT_BUCKET_NAME+"/MapReduce3.jar") // This should be a full map reduce application.
			.withArgs(S3_INTERMIDIATE+(i-1)+inputSize,S3_INTERMIDIATE+i+inputSize);

			StepConfig stepConfig3 = new StepConfig()
			.withName("MapReduce"+i+inputSize)
			.withHadoopJarStep(hadoopJarStep3)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
			System.out.println("Running Step: "+i);
			i++;
			stepsConfigs.add(stepConfig3);
		}
		//----------------------STEP 4-------------------------------
		if(i<=amountOfSteps)
		{
			System.out.println("args[0]= "+argsForJob4);
			HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
			.withJar("s3://"+S3_PROJECT_BUCKET_NAME+"/MapReduce4.jar") // This should be a full map reduce application.
			.withArgs(S3_INTERMIDIATE+(i-1)+inputSize,S3_INTERMIDIATE+i+inputSize, argsForJob4);

			StepConfig stepConfig4 = new StepConfig()
			.withName("MapReduce"+i+inputSize)
			.withHadoopJarStep(hadoopJarStep4)
			.withActionOnFailure("TERMINATE_JOB_FLOW");
			System.out.println("Running Step: "+i);
			i++;
			stepsConfigs.add(stepConfig4);
		}
	}

	public static void configBootStrapActions(
			ArrayList<BootstrapActionConfig> bootstrapActions) {
		List<String> bootstrappingArgs1 = new ArrayList<String>();

		bootstrappingArgs1.add("--yarn-key-value");
		bootstrappingArgs1.add("mapred.tasktracker.map.tasks.maximum=3");

		bootstrapActions.add(new BootstrapActionConfig("Change the maximum number of map tasks",
				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrappingArgs1)));

		List<String> bootstrappingArgs2 = new ArrayList<String>();		
		bootstrappingArgs2.add("--namenode-heap-size=5120");
		bootstrappingArgs2.add("--namenode-opts=-XX:GCTimeRatio=19"); 
		bootstrapActions.add(new BootstrapActionConfig("Set the NameNode heap size",
				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-daemons", bootstrappingArgs2)));

		List<String> bootstrappingArgs3 = new ArrayList<String>();

		bootstrappingArgs3.add("--yarn-key-value");
		bootstrappingArgs3.add("mapred.tasktracker.reduce.tasks.maximum=3");

		bootstrapActions.add(new BootstrapActionConfig("Change the maximum number of reducer tasks",
				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrappingArgs3)));

		List<String> bootstrappingArgs4 = new ArrayList<String>();
		//Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name=,Args=[--yarn-key-value,mapred.tasktracker.map.tasks.maximum=2] 
		bootstrappingArgs4.add("-m");
		bootstrappingArgs4.add("mapred.child.java.opts=-Xmx5120");
//		bootstrappingArgs4.add("mapred.child.java.opts=-Xmx10240");
//		bootstrappingArgs4.add("mapred.child.java.opts=-Xmx2g");
		
		bootstrapActions.add(new BootstrapActionConfig("Increase the JVM heap size and task timeout",
				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrappingArgs4)));
	}

}










