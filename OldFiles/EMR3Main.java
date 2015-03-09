import java.util.ArrayList;

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


public class EMR3Main {

	public static void main(String[] args) {

		System.out.println("EMR3 Main");

		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		//----------------------STEP 1-------------------------------
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce1.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/29Tweets.txt", "s3n://default-ds-ass2-elidor/intermediate1");
		//		    .withMainClass("WordCount")

		StepConfig stepConfig1 = new StepConfig()
		.withName("MapReduce1")
		.withHadoopJarStep(hadoopJarStep1)
		.withActionOnFailure("TERMINATE_JOB_FLOW");

		//----------------------STEP 2-------------------------------
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
		.withJar("s3://default-ds-ass2-elidor/MapReduce2.jar") // This should be a full map reduce application.
		.withArgs("s3n://default-ds-ass2-elidor/intermediate1","s3n://default-ds-ass2-elidor/intermediate2" );


		StepConfig stepConfig2 = new StepConfig()
		.withName("MapReduce2")
		.withHadoopJarStep(hadoopJarStep2)
		.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		
		//----------------------STEP 3-------------------------------
				HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("s3://default-ds-ass2-elidor/MapReduce3.jar") // This should be a full map reduce application.
				.withArgs("s3n://default-ds-ass2-elidor/intermediate2", "s3n://default-ds-ass2-elidor/outputTwiter");


				StepConfig stepConfig3 = new StepConfig()
				.withName("MapReduce3")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		//----------------------JOB FLOW CONFIG------------------------
		ArrayList<StepConfig> stepsConfigs = new ArrayList<StepConfig>();
		stepsConfigs.add(stepConfig1);
		stepsConfigs.add(stepConfig2);
		stepsConfigs.add(stepConfig3);
		
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
		.withSteps(stepsConfigs)
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
