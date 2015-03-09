import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;


public class EMR4Main4GB {

	public static void main(String[] args) {

		System.out.println("EMR4 Main for 4GB file");

		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		//AmazonS3Client s3=new AmazonS3Client(credentials);
//		AmazonSQS sqs = new AmazonSQSClient(credentials);


		//Region usEast3 = Region.getRegion(Regions.US_EAST_1); 
		//sqs.setRegion(usEast3);

		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		

		//----------------------STEP 1-------------------------------
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce1.jar") // This should be a full map reduce application.
		.withArgs("s3n://dsp151/tweetCorpus.txt", "s3n://default-ds-ass2-elidor/4GBintermediate_1");
		//s3n://dsp151/smallCorpus 
		
		
		StepConfig stepConfig1 = new StepConfig()
		.withName("MapReduce1")
		.withHadoopJarStep(hadoopJarStep1)
		.withActionOnFailure("TERMINATE_JOB_FLOW");

		//----------------------STEP 2-------------------------------
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce2.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/4GBintermediate_1","s3n://default-ds-ass2-elidor/4GBintermediate_2" );


		StepConfig stepConfig2 = new StepConfig()
		.withName("MapReduce2")
		.withHadoopJarStep(hadoopJarStep2)
		.withActionOnFailure("TERMINATE_JOB_FLOW");


		//----------------------STEP 3-------------------------------
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce3.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/4GBintermediate_2", "s3n://default-ds-ass2-elidor/4GBintermediate_3");


		StepConfig stepConfig3 = new StepConfig()
		.withName("MapReduce3")
		.withHadoopJarStep(hadoopJarStep3)
		.withActionOnFailure("TERMINATE_JOB_FLOW");

		//----------------------STEP 4-------------------------------
		System.out.println("args[0]= "+args[0]);
		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce4.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/4GBintermediate_3", "s3n://default-ds-ass2-elidor/4GBoutputTwiter", args[0]);


		StepConfig stepConfig4 = new StepConfig()
		.withName("MapReduce4")
		.withHadoopJarStep(hadoopJarStep4)
		.withActionOnFailure("TERMINATE_JOB_FLOW");

		//----------------------JOB FLOW CONFIG------------------------
		ArrayList<StepConfig> stepsConfigs = new ArrayList<StepConfig>();
		stepsConfigs.add(stepConfig1);
		stepsConfigs.add(stepConfig2);
		stepsConfigs.add(stepConfig3);
		stepsConfigs.add(stepConfig4);


		ArrayList<BootstrapActionConfig> bootstrapActions = new ArrayList<BootstrapActionConfig>();
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
		bootstrappingArgs3.add("mapred.tasktracker.reduce.tasks.maximum=1");

		bootstrapActions.add(new BootstrapActionConfig("Change the maximum number of reducer tasks",
				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrappingArgs3)));



		//		List<String> bootstrappingArgs4 = new ArrayList<String>();
		//		bootstrappingArgs4.add("mapred.child.java.opts=-Xmx5120");
		//		bootstrapActions.add(new BootstrapActionConfig("Increase the JVM heap size and task timeout",
		//				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrappingArgs4)));

		/*List<String> bootstrappingArgs4 = new ArrayList<String>();

		bootstrappingArgs4.add("mapred.child.java.opts=-Xmx5120");

		bootstrapActions.add(new BootstrapActionConfig("ncrease the JVM heap size and task timeou",
				new ScriptBootstrapActionConfig("s3://elasticmapreduce/bootstrap-actions/configure-hadoop", bootstrappingArgs4)));*/

		List<String> bootstrappingArgs4 = new ArrayList<String>();
		//Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name=,Args=[--yarn-key-value,mapred.tasktracker.map.tasks.maximum=2] 
		bootstrappingArgs4.add("-m");


		/*RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
												.withName("fourMapReduceJobs")
												.withInstances(instances)
												.withSteps(stepsConfigs)
												.withLogUri("s3n://dsp151/mapreduce/logs/")
												.withBootstrapActions(bootstrapActions);*/




		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		.withInstanceCount(7)
		.withMasterInstanceType(InstanceType.M3Xlarge.toString())
		.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
		.withHadoopVersion("2.4.0").withEc2KeyName("first_instance_ds")
		.withKeepJobFlowAliveWhenNoSteps(false)
		.withPlacement(new PlacementType("us-east-1e"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		.withName("4MapReduceJobs4GB")
		.withInstances(instances)
		.withSteps(stepsConfigs)
		.withLogUri("s3n://default-ds-ass2-elidor/logs/")
		.withBootstrapActions(bootstrapActions);

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);




		/*
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/TweetTest.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/29Tweets.txt", "s3n://default-ds-ass2-elidor/outputTwiter");
		//		    .withMainClass("WordCount")

		StepConfig stepConfig1 = new StepConfig()
		.withName("MapReduce1")
		.withHadoopJarStep(hadoopJarStep1)
		.withActionOnFailure("TERMINATE_JOB_FLOW");*/

	}
}
