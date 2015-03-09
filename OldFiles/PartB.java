import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import Test.AWSObject;

import com.amazonaws.services.s3.model.S3Object;


public class PartB {

	public static void main(String[] args) throws IOException 
	{


		AWSObject awsO = new AWSObject();
		awsO.initS3();

		S3Object object = awsO.S3DownloadFile("default-ds-ass2-elidor", "29Tweets.txt");

		//Add every line  from the downloaded summary file to a List
		InputStream input = object.getObjectContent();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));


		String originalTweet = reader.readLine();
		System.out.println("0) "+originalTweet);

		Vector<String> relevantTweets;
		for (int i = 0; i < 10; i++) {
			relevantTweets = new Vector<String>();

			String line;
			while (true) {
				line = reader.readLine();
				if (line == null)
				{
					break;
				}
				else if (line.contains("\t")) //its a similar tweet to orginal tweet
				{
					relevantTweets.add(line);
					System.out.println("\t"+line);
				}
				else
				{
					averagePrecision(originalTweet,relevantTweets);
					originalTweet = line;
					System.out.println((i+1)+") "+line);
					break;
				}


			}
		}


	}


	public static double averagePrecision(String tweet, Vector<String> similarTweets) {
		double avergPrecision = 0;
		double similarity = 0;
		double t = 0;
		double vecSize = similarTweets.size();
		double similarutyCounter = 0;
		double iterationCounrter = 0;
		for (String similarTweet : similarTweets) {
			try {
				System.out.println("\npress 1 if relevant, else 0");
				System.out.println(tweet);
				System.out.println(similarTweet);
				iterationCounrter++;
				similarity = System.in.read();
				t = System.in.read();
				t = System.in.read();
				similarity -= 48;
			} catch (IOException e) {
				e.printStackTrace();
			}
			similarutyCounter += similarity;
			avergPrecision += similarutyCounter/iterationCounrter;
			//			System.out.println("avergPrecision:" + similarutyCounter/iterationCounrter);
		}
		return avergPrecision/vecSize;
	}

	

}
