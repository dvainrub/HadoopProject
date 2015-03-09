import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class EMR1Main {

	public static void main(String[] args) {
		System.out.println("EMR1 Main");

		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		//----------------------STEP 1-------------------------------
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce1.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/29Tweets.txt", "s3n://default-ds-ass2-elidor/outputTwiter");
		//		    .withMainClass("WordCount")

		StepConfig stepConfig1 = new StepConfig()
		.withName("MapReduce1")
		.withHadoopJarStep(hadoopJarStep1)
		.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		//----------------------JOB FLOW CONFIG------------------------

		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		.withInstanceCount(2)
		.withMasterInstanceType(InstanceType.M3Xlarge.toString())
		.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
		.withHadoopVersion("2.4.0").withEc2KeyName("first_instance_ds")
		.withKeepJobFlowAliveWhenNoSteps(false)
		.withPlacement(new PlacementType("us-east-1e"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		.withName("twoMapReduceJobs")
		.withInstances(instances)
		.withSteps(stepConfig1)
		.withLogUri("s3n://default-ds-ass2-elidor/logs/");

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
