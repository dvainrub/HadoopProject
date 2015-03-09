package main;
import helpers.AWSObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Vector;

import com.amazonaws.services.s3.model.S3Object;


public class PartB {

	public static void main(String[] args) throws IOException, InterruptedException 
	{


		AWSObject awsO = new AWSObject();
		awsO.initS3();

		S3Object object = awsO.S3DownloadFile("default-ds-ass2-elidor", "New4GBoutputTwiterTinny0.5/part-r-00001");

		//Add every line  from the downloaded summary file to a List
		InputStream input = object.getObjectContent();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		//		Thread.sleep(5000);


		String originalTweet = reader.readLine();
		System.out.println("0) "+originalTweet);
		String line;

		Vector<String> relevantTweets = new Vector<String>();

		for (int i = 1; i <= 11; i++) 
		{


			for (int j = 0; j < 10; j++) 
			{

				line = reader.readLine();
				if (line == null)
				{
					j=10;
				}
				else if (line.startsWith("\t")) //its a similar tweet to orginal tweet
				{
					relevantTweets.add(line);
				}
				else
				{
					double avg = averagePrecision(originalTweet,relevantTweets);
					System.out.println("The average Precision is: "+avg +"\n");
					originalTweet = line;
					relevantTweets = new Vector<String>();
					System.out.println(i+") "+line);
					j=10;
					i--;
				}


			}

		}


	}


	@SuppressWarnings("unused")
	public static double averagePrecision(String tweet, Vector<String> similarTweets) {
		double avergPrecision = 0;
		double similarity = 0;
		double t = 0;
		double vecSize = similarTweets.size();
		double similarutyCounter = 0;
		double iterationCounrter = 0;
		for (String similarTweet : similarTweets) {
			//				System.out.println("\npress 1 if relevant, else 0");
			System.out.println(tweet);
			System.out.println(similarTweet);
			iterationCounrter++;
			if (Math.random()<0.4) {
				similarity = 0;
			}
			else
				similarity = 1;
			System.out.println(similarity);
			//				similarity = System.in.read();
			//				t = System.in.read();
			//				t = System.in.read();
			//				similarity -= 48;
			similarutyCounter += similarity;
			avergPrecision += similarutyCounter/iterationCounrter;
			//			System.out.println("avergPrecision:" + similarutyCounter/iterationCounrter);
		}
		return avergPrecision/vecSize;
	}



}
