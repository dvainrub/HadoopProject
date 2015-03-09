package helpers;


import java.io.File;
import java.io.IOException;

public class AWSHelper {


	private static final String S3_PROJECT_BUCKET_NAME = "dsps151-ass2-elidor";

	public static void main(String[] args) {
		System.out.println("AWS HELPER");

		int a=0;
		System.out.println("1-deleteS3Buckets");
		System.out.println("2-uploadJars");
		System.out.println("3-everything");

		try {
			a = System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}

		AWSObject awsO = new AWSObject();
		awsO.initS3();

		if (a==49){
			System.out.println("-->1");
			deleteS3Buckets(awsO);
		}
		else if (a==50)
		{
			System.out.println("-->2");
			uploadJars(awsO);
		}
		else if (a==51)
		{
			System.out.println("-->3");
			deleteS3Buckets(awsO);
			uploadJars(awsO);
		}
		else
			System.out.println("!!Nothing selected");

		System.out.println("\n -------DONE--------");
	}

	private static void uploadJars(AWSObject awsO) {
		awsO.S3UploadFile(S3_PROJECT_BUCKET_NAME, "29Tweets.txt", new File("29Tweets.txt"));
		System.out.println("29Tweets.txt Uploaded");

		awsO.S3UploadFile(S3_PROJECT_BUCKET_NAME, "MapReduce1.jar", new File("MapReduce1.jar"));
		System.out.println("MapReduce1 Jar Uploaded");

		awsO.S3UploadFile(S3_PROJECT_BUCKET_NAME, "MapReduce2.jar", new File("MapReduce2.jar"));
		System.out.println("MapReduce2 Jar Uploaded");

		awsO.S3UploadFile(S3_PROJECT_BUCKET_NAME, "MapReduce3.jar", new File("MapReduce3.jar"));
		System.out.println("MapReduce3 Jar Uploaded");

		awsO.S3UploadFile(S3_PROJECT_BUCKET_NAME, "MapReduce4.jar", new File("MapReduce4.jar"));
		System.out.println("MapReduce4 Jar Uploaded");	
	}

	private static void deleteS3Buckets(AWSObject awsO) {
		try {
			awsO.S3ForceDeleteBucket(S3_PROJECT_BUCKET_NAME);
			System.out.println(S3_PROJECT_BUCKET_NAME +" bucket deleted");
		} catch (Exception e) {}

		/*try{
			awsO.S3ForceDeleteBucket("aws-logs-512945184143-us-east-1");
			System.out.println("aws-logs-512945184143-us-east-1 bucket deleted");
		} catch (Exception e){e.printStackTrace();}*/

	}
}
