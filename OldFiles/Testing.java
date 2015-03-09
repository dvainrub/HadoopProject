import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class Testing {

	private static Text tweet = new Text();
	private static String[] filteredWords;
	private static Set<String> set;

	public static void main(String[] args) {
		String s = "{\"created_at\":\"Fri Jan 09 13:07:30 +0000 2015\",\"id\":553538557395357696,\"id_str\":\"553538557395357696\",\"text\":\"Cartoonists react to the shooting at Charlie Hebdo http:\\/\\/t.co\\/pU38xbgz56\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":61327086,\"id_str\":\"61327086\",\"name\":\"Sara Perlas Chapas\",\"screen_name\":\"selanora\",\"location\":\"Fight Club\",\"url\":\"http:\\/\\/selanora.tumblr.com\\/\",\"description\":\"I can be the answer\",\"protected\":false,\"verified\":false,\"followers_count\":1413,\"friends_count\":274,\"listed_count\":7,\"favourites_count\":81,\"statuses_count\":33328,\"created_at\":\"Wed Jul 29 23:34:33 +0000 2009\",\"utc_offset\":-10800,\"time_zone\":\"Greenland\",\"geo_enabled\":false,\"lang\":\"es\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"FFFFFF\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000179748341\\/iQ5lfRLW.jpeg\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000179748341\\/iQ5lfRLW.jpeg\",\"profile_background_tile\":true,\"profile_link_color\":\"14C7B8\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"D15513\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/537752339227377664\\/LMPWsPSy_normal.jpeg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/537752339227377664\\/LMPWsPSy_normal.jpeg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/61327086\\/1350593225\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"trends\":[],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/pU38xbgz56\",\"expanded_url\":\"http:\\/\\/9gag.com\\/gag\\/ad6zOVB?ref=android.s.tw\",\"display_url\":\"9gag.com\\/gag\\/ad6zOVB?re\\u2026\",\"indices\":[51,73]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"medium\",\"lang\":\"en\",\"timestamp_ms\":\"1420808850989\"}";
		TweetKey tk = new TweetKey(s,0);
		TweetValue tv = new TweetValue(s,0);

		

		String stk ="Fri Jan 09 13:07:30 +0000 2015##karish##553538557395357696";
		String stv = "Sara Perlas Chapas##karish##Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56##karish##false##karish##false";

		/*System.out.println(tk+"\n");
		System.out.println(tv);

		map1Test(tk,tv);

		reduce1Test("Cartoonists",
				"Fri Jan 09 13:07:30 +0000 2015##karish##553538557395357696##tiburon##Sara Perlas Chapas##karish##Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56##karish##false##karish##false");
	*/
		map2Test(0,
				"Police\t1##shark##Fri Jan 09 16:57:54 +0000 2015##karish##553596536139943936##tiburon##WILLIAM\tBROWN##karish##Watch: Police shoot-out with Kouachi brothers: Gunfire and explosions at printing plant in Dammartin-en-Goele where Charlie Hebdo Par...##karish##false##karish##false");
	
	}

	public static void map1Test(TweetKey key, TweetValue value)
	{
		tweet = value.getTweetText();

		String filteredTweet = Filter.filterStopWords(tweet.toString());
		filteredWords = filteredTweet.split(" ");

		set = new TreeSet<String>();
		for (int i = 0; i < filteredWords.length; i++) 
		{
			set.add(filteredWords[i]);
		}

		String s = null;
		for (String uniqueWord : set) {
			/*arr = new Writable[2];
			arr[0] = new TweetKey(key);
			arr[1] = new TweetValue(value);*/

			s = key.toString() +TweetMapReduce.BETWEEN_KEY_AND_VALUE_DELIMETER+ value.toString();

			System.out.println(uniqueWord); //Cartoonists
			System.out.println(s);//Fri Jan 09 13:07:30 +0000 2015##karish##553538557395357696##tiburon##Sara Perlas Chapas##karish##Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56##karish##false##karish##false
		}
	}



	public static void reduce1Test(String word, String tweetKeyValue) {
		System.out.println("reduce1Test------");
		int amountOfTweets=0;
		StringBuilder sb = new StringBuilder();
		
		sb.append(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER + tweetKeyValue);
		amountOfTweets++;

		sb.insert(0, amountOfTweets);

		//			long inputs = context.getCounter(Counters.gMAP_INPUT_RECORDS).getValue();
		//			String s = String.valueOf(inputs);
		//			
		//			context.write(new Text("Map Input Records"), new Text(s));

		
		System.out.println(word);//Cartoonists
		System.out.println(sb.toString());//1##shark##Fri Jan 09 13:07:30 +0000 2015##karish##553538557395357696##tiburon##Sara Perlas Chapas##karish##Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56##karish##false##karish##false
	}
	
	public static void map2Test(long  longKey, String mapOfTuplesAndNumberOfTweetsThatContainTheWord) 
	{
		System.out.println("map2Test-------------");
		String[] valueAndKey = mapOfTuplesAndNumberOfTweetsThatContainTheWord.split("\t", 2);
		String word = valueAndKey[0];
		if(valueAndKey.length==2){
			String[] tuplesOfTweets = valueAndKey[1].split(TweetMapReduce.BETWEEN_TUPLES_OF_KEYVALUE_DELIMETER);
			double amountOfTweetsThatContainTheWord = Integer.valueOf(tuplesOfTweets[0]);

			//TODO--CALCULATE TOTAL AMOUT OF TWEETS WITH COUNTER
			long mapInputRecordsCounterValue = 100;
			double idf = calc_idf(mapInputRecordsCounterValue, amountOfTweetsThatContainTheWord); //same for every tweet

			TweetKey[] key = new TweetKey[tuplesOfTweets.length-1];
			TweetValue[] value = new TweetValue[tuplesOfTweets.length-1];

			String[] keyAndValue;
			String mapKey, mapValue;
			for (int i = 0; i < key.length ; i++)
			{
				keyAndValue = tuplesOfTweets[i+1].split(TweetMapReduce.BETWEEN_KEY_AND_VALUE_DELIMETER);
				key[i] = new TweetKey(keyAndValue[0]);
				value[i] = new TweetValue(keyAndValue[1]);

				double tf = value[i].calc_tf(word);
				double tf_idf = tf * idf;

				mapKey = key[i].toString();
				mapValue = value[i].toString()+TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER+word+TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER+tf_idf;

				System.out.println(mapKey);//Fri Jan 09 13:07:30 +0000 2015##karish##553538557395357696
				System.out.println(mapValue);//Sara Perlas Chapas##karish##Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56##karish##false##karish##false##nena##Cartoonists##eliyahuRon##0.7675283643313486

				reduce2Test(mapKey, mapValue);
			}
		}
	}


	//tf(t, d) = LOG(#all tweets/total num of d that contain t)
	//t = term, d = tweet,
	private static double calc_idf(long mapInputRecordsCounterValue, double amountOfTweetsThatContainTheWord) {
		return Math.log(mapInputRecordsCounterValue/amountOfTweetsThatContainTheWord);
	}
	
	public static void reduce2Test(String tweetKey, String tweetValueAndWordTfIdf) {

		StringBuilder sb = new StringBuilder();
		String[] s = null;
		
			s = tweetValueAndWordTfIdf.split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER);
			sb.append(TweetMapReduce.BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER + s[1]);
		
		sb.append(TweetMapReduce.BETWEEN_TFIDF_VEC_AND_NORMA_DELIMETER + normaA(sb.toString()));
		sb.insert(0, s[0] + TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER);
		System.out.println(tweetKey);
		System.out.println(sb.toString());
	}

	//calculate the norma of A
	public static double normaA(String a) {
		double sum = 0;
		String[] aVec = a.split(TweetMapReduce.BETWEEN_WORD_AND_TFIDF_DELIMETER);

		double tfIdf = 0;
		for (int indexA = 1; indexA < aVec.length; indexA++) {
			tfIdf = Double.valueOf(aVec[indexA].split(TweetMapReduce.BETWEEN_PAIRS_OF_WORD_TFIDF_DELIMETER)[0]);
			sum += Math.pow(tfIdf, 2);
		}
		return Math.sqrt(sum);
	}




}
