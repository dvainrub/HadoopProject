package main;
import java.util.ArrayList;

import tweet.StaticVars;
import tweet.StaticVars.InputSize;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class LocalEMRInitiator {
	static final String className = "ElasticMapReduce";

	public static void main(String[] args) {
		System.out.println(className);

		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		//----------------------JOB FLOW CONFIG------------------------
		ArrayList<StepConfig> stepsConfigs = new ArrayList<StepConfig>();
		//Big/Medium/Small or Default -> Corpus Input Size or Default Input
		//Args[0] -> Amount of similar tweets N
		//Number 1-4 steps to initialize
		InputSize inputSize = InputSize.Small;
		StaticVars.configHadoopJarSteps(stepsConfigs, inputSize, args[0], 4);
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
		.withInstanceCount(6)
		.withMasterInstanceType(InstanceType.M3Xlarge.toString())
		.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
//		.withSlaveInstanceType(InstanceType.M32xlarge.toString())
		.withHadoopVersion("2.4.0").withEc2KeyName(StaticVars.EC2_INSTANCE_KEY_NAME)
		.withKeepJobFlowAliveWhenNoSteps(false)
		.withPlacement(new PlacementType("us-east-1e"));
		
		//------------------BOOTSTRAP ACTIONS---------------
		ArrayList<BootstrapActionConfig> bootstrapActions = new ArrayList<BootstrapActionConfig>();
		StaticVars.configBootStrapActions(bootstrapActions);
		System.out.println("Bootstrap Complete");
		
		
		//--------------------RUN JOB----------------------------
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		.withName(className+inputSize)
		.withInstances(instances)
		.withSteps(stepsConfigs)
		.withBootstrapActions(bootstrapActions)
		.withLogUri(StaticVars.S3_LOG);

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);

	}

	

	
}
