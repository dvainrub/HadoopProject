import java.util.TreeSet;

import org.apache.hadoop.io.Text;


public class Tests {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		TreeSet<String> similarityTweets = new TreeSet<>();
		similarityTweets.add("1");
		similarityTweets.add("3");
		similarityTweets.add("2");
		for (String tweetAndCosineSimilar : similarityTweets) {
			System.out.println(tweetAndCosineSimilar);
		}
		while (!similarityTweets.isEmpty()) {
			System.out.println(similarityTweets.pollFirst());
		}
		System.out.println(similarityTweets.size());
	}

}
