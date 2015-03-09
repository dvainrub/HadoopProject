package Test;


import java.io.File;
import java.io.IOException;

public class AWSHelper {


	public static void main(String[] args) {
		
		
		
		/*
		String s = "	4##sh";
		String[] str = s.split("\t", 2);
		System.out.println(str[0]);
		System.out.println(str[1]);
		
		String s2 = "  ";
		String[] str2 = s2.split(" ", 2);
		System.out.println(str2[0]);
		System.out.println(str2[1]);*/
		
		/*String s2 = "FREE?\nh";
		String s3 = "FREE?\n hey";
		
		System.out.println(s2.replaceAll("\n", "##SlashNDelimeter##"));
		System.out.println(s3.replaceAll("\n", "##SlashNDelimeter##"));*/
		
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
		File tweets29 = new File("29Tweets.txt");
		awsO.S3UploadFile("default-ds-ass2-elidor", "29Tweets.txt", tweets29);
		System.out.println("29Tweets.txt Uploaded");

		File MapReduce1Jar = new File("MapReduce1.jar");
		awsO.S3UploadFile("default-ds-ass2-elidor", "MapReduce1.jar", MapReduce1Jar);
		System.out.println("MapReduce1 Jar Uploaded");
		
		File MapReduce2Jar = new File("MapReduce2.jar");
		awsO.S3UploadFile("default-ds-ass2-elidor", "MapReduce2.jar", MapReduce2Jar);
		System.out.println("MapReduce2 Jar Uploaded");
		
		File MapReduce3Jar = new File("MapReduce3.jar");
		awsO.S3UploadFile("default-ds-ass2-elidor", "MapReduce3.jar", MapReduce3Jar);
		System.out.println("MapReduce3 Jar Uploaded");
		
		File MapReduce4Jar = new File("MapReduce4.jar");
		awsO.S3UploadFile("default-ds-ass2-elidor", "MapReduce4.jar", MapReduce4Jar);
		System.out.println("MapReduce4 Jar Uploaded");	


		/*System.out.println("!!!!DONT FORGET TO CHANGE THE PERMISSIONS->POLICY OF THE BUCKET!!!");*/
		/*
		 {
"Id": "Policy1424209498483",
"Statement": [
{
"Sid": "Stmt1424209492671",
"Action": [
"s3:GetObject"
],
"Effect": "Allow",
"Resource": "arn:aws:s3:::applicationcode-ds-151-elidor/*",
"Principal": "*"
}
]
} */
	}

	private static void deleteS3Buckets(AWSObject awsO) {
		try {
			awsO.S3ForceDeleteBucket("default-ds-ass2-elidor");
			System.out.println("default-ds-ass2-elidor bucket deleted");
		} catch (Exception e) {}

		/*try{
			awsO.S3ForceDeleteBucket("aws-logs-512945184143-us-east-1");
			System.out.println("aws-logs-512945184143-us-east-1 bucket deleted");
		} catch (Exception e){e.printStackTrace();}*/

	}

}
