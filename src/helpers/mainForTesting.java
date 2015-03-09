package helpers;



public class mainForTesting {

	public static void main(String[] args) {

		/*String rawTweet = "{\"created_at\":\"Fri Jan 09 13:07:30 +0000 2015\",\"id\":553538557395357696,\"id_str\":\"553538557395357696\",\"text\":\"Cartoonists react to the shooting at Charlie Hebdo http:\\/\\/t.co\\/pU38xbgz56\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":61327086,\"id_str\":\"61327086\",\"name\":\"Sara Perlas Chapas\",\"screen_name\":\"selanora\",\"location\":\"Fight Club\",\"url\":\"http:\\/\\/selanora.tumblr.com\\/\",\"description\":\"I can be the answer\",\"protected\":false,\"verified\":false,\"followers_count\":1413,\"friends_count\":274,\"listed_count\":7,\"favourites_count\":81,\"statuses_count\":33328,\"created_at\":\"Wed Jul 29 23:34:33 +0000 2009\",\"utc_offset\":-10800,\"time_zone\":\"Greenland\",\"geo_enabled\":false,\"lang\":\"es\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"FFFFFF\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000179748341\\/iQ5lfRLW.jpeg\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000179748341\\/iQ5lfRLW.jpeg\",\"profile_background_tile\":true,\"profile_link_color\":\"14C7B8\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_color\":\"D15513\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/537752339227377664\\/LMPWsPSy_normal.jpeg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/537752339227377664\\/LMPWsPSy_normal.jpeg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/61327086\\/1350593225\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"trends\":[],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/pU38xbgz56\",\"expanded_url\":\"http:\\/\\/9gag.com\\/gag\\/ad6zOVB?ref=android.s.tw\",\"display_url\":\"9gag.com\\/gag\\/ad6zOVB?re\\u2026\",\"indices\":[51,73]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"medium\",\"lang\":\"en\",\"timestamp_ms\":\"1420808850989\"}";

		TweetKey tk = new TweetKey(rawTweet,0);
		TweetValue tv = new TweetValue(rawTweet,0);

		TweetKey tk2 = new TweetKey(tk.toString());
		TweetValue tv2 = new TweetValue(tv.toString());

		Tweet t = new Tweet(tk2,tv2);

		System.out.println(t);*/
		//{"tweetKey":{"createdAt":"Fri Jan 09 13:07:30 +0000 2015","tweetId":"553538557395357696"},
		//"tweetValue":{"text":"Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56","user":"Sara Perlas Chapas","favorited":false,"retweeted":false}}

		//{"tweetKey":"{\"createdAt\":\"Fri Jan 09 13:07:30 +0000 2015\",\"tweetId\":\"553538557395357696\"}",
		//"tweetValue":"{\"text\":\"Cartoonists react to the shooting at Charlie Hebdo http://t.co/pU38xbgz56\",\"user\":\"Sara Perlas Chapas\",\"favorited\":false,\"retweeted\":false}"}

		/*
		 * 	String reduce1Output = "\t[#Android, 4,"
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:47 +0000 2015\",\"tweetId\":\"553523273720295425\"},"
				+ "\"tweetValue\":{\"text\":\"Criminal is under arrest! Fight the gangs in the streets of London! http://t.co/6vauasIpLq #Android #AndroidGames #GameInsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"קרן גלעדי\"}},"
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523258931556352\"},"
				+ "\"tweetValue\":{\"text\":\"I've collected $2465! Think you can do better? http://t.co/BHKcJ5TawE #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"yana\"}}, "
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:44 +0000 2015\",\"tweetId\":\"553523264040210432\"},"
				+ "\"tweetValue\":{\"text\":\"RT @3CX: Check out the new look for 3CXPhone for #Android - coming soon with #3CX Phone System 12.5 http://t.co/ws7Q1xyHY3 http://t.co/Eu0v\\u2026\",\"retweeted\":false,\"favorited\":false,\"user\":\"Mattias Kressmark\"}},"
				+ "{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523257471926272\"},"
				+ "\"tweetValue\":{\"text\":\"I finished the \\\"Shortage\\\" task in Big Business Deluxe for Android! http://t.co/aUC6ASoIxa #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"Igor Klašnja\"}}]";

		String[] map2firstSplit = reduce1Output.split("\t",2); 
		//The input text for the map is a line with a tab and then all the output from Reduce1
		//The information we need will be in the second cell of the array
		String mapReduce1CleanOutput = map2firstSplit[1];

		//The output from Reduce1 is of the form: (word, amounts of Tweets in wich w appears, every tweet where w appears)
		//So we separate the information in 3 cells
		String[] map2secondSplit = mapReduce1CleanOutput.split(", ", 2);

		//We take a substring starting from index 1 from the first two cells because they start with an empty space
		String word = map2secondSplit[0].substring(1);
		System.out.println(word);

		String [] map2thirdSplit = map2secondSplit[1].split(",",2);
		double amountOfTimesWordAppears = Double.valueOf(map2thirdSplit[0]);
		System.out.println(amountOfTimesWordAppears);

		//Third cell are all the tweets containing the word, each one starts with "tweetKey:"
		String[] allTweetsContainningTheWord = map2thirdSplit[1].split("\\}\\},");

		for (int i = 0; i < allTweetsContainningTheWord.length; i++) {
			String s = allTweetsContainningTheWord[i]+"}}"; //We remove them in the split so we add them here again
			Tweet tweet = new Tweet (s);
			System.out.println(tweet.getTweetKey().createdAt);
		}*/

		/*String reduce2Output = "[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:38 +0000 2015\",\"tweetId\":\"553523238525861890\"},"
				+ "\"tweetValue\":{\"text\":\"Having fun playing #CSRRacing for Android, why not join me for FREE?\\nhttp://t.co/h0EJsG8i4P  xxx\",\"retweeted\":false,\"favorited\":false,\"user\":\"nguyennam78\"}}, "
				+ "0.9801108198045045, "
				+ "[fun, 0.36201072644683135], [#CSRRacing, 0.36201072644683135], [, 0.20797801965573237], [playing, 0.36201072644683135], [join, 0.36201072644683135], [xxx, 0.36201072644683135], [Having, 0.36201072644683135], [Android,, 0.36201072644683135]]";

		String[] map3firstSplit = reduce2Output.substring(1).split("\\}\\}, ");
		String tweetString = map3firstSplit[0]+"}}";
		System.out.println(tweetString);

		String[] map3secondSplit = map3firstSplit[1].split(",",2);
		double norma = Double.valueOf(map3secondSplit[0]);
		System.out.println(norma);*/

		//substring to eliminate the last "]"
		//split to reparate each word-tfidf pair
		/*	String[] map3thirdSplit = map3secondSplit[1].substring(0, map3secondSplit[1].length()-1).split("]");
		String cleanPair;
		for (String wordTfidfPair : map3thirdSplit) {
			//We remove things like space or ',' at the beginning of the pair
			cleanPair = wordTfidfPair.substring(2);
			System.out.println(cleanPair);
			if (cleanPair.substring(0,1).equals("[")) //If the pair starts with "[" we remove it
			{
				System.out.println(cleanPair.substring(1));

			}
		}*/
		//		String []
		//		System.out.println(map3firstSplit[1].split(", ",2)[0]);
		/*
		Tweet tweet = new Tweet(tweetString);
		System.out.println(tweet);
		String text = tweet.getTweetValue().getTweetText().toString();
		System.out.println(text);
		String filteredTweet = Filter.filterStopWords(text.toString());
		String[] filteredWords = filteredTweet.split(" ");

		Set<String> set = new TreeSet<String>();
		for (int i = 0; i < filteredWords.length; i++) 
		{
			set.add(filteredWords[i]);
		}

		for (String uniqueWord : set) {
			if(!TweetMapReduce.problematicWord(uniqueWord))
			{
				System.out.println(uniqueWord);
				System.out.println(reduce2Output);
				//context.write(new Text(uniqueWord), new Tweet(new TweetKey(key),new TweetValue(value)));
			}
		}*/

		/*String reduce3Output = "\t[#Android, "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:47 +0000 2015\",\"tweetId\":\"553523273720295425\"},\"tweetValue\":{\"text\":\"Criminal is under arrest! Fight the gangs in the streets of London! http://t.co/6vauasIpLq #Android #AndroidGames #GameInsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"קרן גלעדי\"}}, 0.9951903378201662, [#Android, 0.18718021769015913], [streets, 0.3258096538021482], [#GameInsight, 0.3258096538021482], [gangs, 0.3258096538021482], [arrest!, 0.3258096538021482], [Fight, 0.3258096538021482], [http://t.co/6vauasIpLq, 0.3258096538021482], [London!, 0.3258096538021482], [#AndroidGames, 0.3258096538021482], [Criminal, 0.3258096538021482]], "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523258931556352\"},\"tweetValue\":{\"text\":\"I've collected $2465! Think you can do better? http://t.co/BHKcJ5TawE #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"yana\"}}, 0.8637462516301732, [I've, 0.1466337068793427], [better?, 0.3258096538021482], [#Androidgames, 0.2564949357461537], [Think, 0.3258096538021482], [can, 0.3258096538021482], [#Gameinsight, 0.2564949357461537], [collected, 0.16486586255873817], [#Android, 0.18718021769015913], [$2465!, 0.3258096538021482], [http://t.co/BHKcJ5TawE, 0.3258096538021482]], "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:43 +0000 2015\",\"tweetId\":\"553523257471926272\"},\"tweetValue\":{\"text\":\"I finished the \\\"Shortage\\\" task in Big Business Deluxe for Android! http://t.co/aUC6ASoIxa #Androidgames #Android #Gameinsight\",\"retweeted\":false,\"favorited\":false,\"user\":\"Igor Klašnja\"}}, 0.916258643925171, [Deluxe, 0.2961905943655893], [Big, 0.2961905943655893], [Android!, 0.2961905943655893], [finished, 0.2961905943655893], [#Android, 0.17016383426378104], [#Gameinsight, 0.23317721431468516], [http://t.co/aUC6ASoIxa, 0.2961905943655893], [task, 0.2961905943655893], [\"Shortage\", 0.2961905943655893], [Business, 0.2961905943655893], [#Androidgames, 0.23317721431468516]], "
				+ "##TWEET_LIST##[{\"tweetKey\":{\"createdAt\":\"Fri Jan 09 12:06:44 +0000 2015\",\"tweetId\":\"553523264040210432\"},\"tweetValue\":{\"text\":\"RT @3CX: Check out the new look for 3CXPhone for #Android - coming soon with #3CX Phone System 12.5 http://t.co/ws7Q1xyHY3 http://t.co/Eu0v\\u2026\",\"retweeted\":false,\"favorited\":false,\"user\":\"Mattias Kressmark\"}}, 0.7724339840392499, [#Android, 0.11698763605634946], [Phone, 0.20363103362634263], [coming, 0.16030933484134605], [look, 0.20363103362634263], [soon, 0.20363103362634263], [-, 0.20363103362634263], [12.5, 0.20363103362634263], [3CXPhone, 0.20363103362634263], [http://t.co/ws7Q1xyHY3, 0.20363103362634263], [new, 0.20363103362634263], [http://t.co/Eu0v…, 0.20363103362634263], [System, 0.20363103362634263], [RT, 0.13496776558458576], [Check, 0.20363103362634263], [#3CX, 0.20363103362634263], [@3CX:, 0.20363103362634263]]]";

		String [] map4firstSplit = reduce3Output.split("\t",2);

		String mapReduce4CleanOutput;
		if (map4firstSplit.length!=2){
//			This comes from a "damaged tweet, for example something like:
//			[
//			\n\n   or  \n
//			#Android, .....]
//			And we received only "#Android, .....]", so we add "[" at the beginning to proceed
//			
			mapReduce4CleanOutput = "["+map4firstSplit[0];
		}
		else {
			//not damaged input
			mapReduce4CleanOutput = map4firstSplit[1];
		}

		//The output from Reduce3 is of the form: (word, TweetList, TweetList,...,TweetList)
		//We separate the first word from the rest with ",\space" in two cells:
		String[] map4secondSplit = mapReduce4CleanOutput.split(", ", 2);


		String [] map4thirdSplit = map4secondSplit[1].split(StaticVars.TWEET_LIST_DELIMITER);
		MyMapWritable collectionOfTweetsMap = new MyMapWritable();
		MyMapWritable tweetMap =  new MyMapWritable();
		MyMapWritable vectorMap =  new MyMapWritable();

		MapReduce4.Map.convertToMapDataStructure(map4thirdSplit, collectionOfTweetsMap, tweetMap, vectorMap);
//		System.out.println(collectionOfTweetsMap);

//		System.out.println(collectionOfTweetsMap.length());

		for(int i = 0; i < collectionOfTweetsMap.length(); i++){
			for(int j = 0; j < collectionOfTweetsMap.length(); j++){
				if(i != j)
				{
					MyMapWritable tweetMapA = (MyMapWritable) collectionOfTweetsMap.get(new IntWritable(i));
					MyMapWritable tweetMapB = (MyMapWritable) collectionOfTweetsMap.get(new IntWritable(j));

					double cosineSimilarity = MapReduce4.Map.calcCosineSimilarity(tweetMapA, tweetMapB);

					Tweet tweetA = (Tweet) tweetMapA.get(new IntWritable(0));
					Tweet tweetB = (Tweet) tweetMapB.get(new IntWritable(0));

					MyMapWritable tweetAndCosSim = new MyMapWritable();
					tweetAndCosSim.put(new IntWritable(0), tweetB);
					tweetAndCosSim.put(new IntWritable(1), new DoubleWritable(cosineSimilarity));
					System.out.println(tweetA);
					System.out.println();
					System.out.println(tweetAndCosSim);

				//String tweetAndCosineSimilarity = calcCosineSimilarity(tweets[i].split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[1], tweets[j].split(TweetMapReduce.BETWEEN_VALUE_AND_WORD_TFIDF_DELIMETER)[1]) + TweetMapReduce.BETWEEN_COSINESIMILARITY_AND_TWEET_DELIMETER + tweets[j];
				//	context.write(new Text(tweets[i]), new Text(tweetAndCosineSimilarity));
				}
			}
		}
		 */
	}
}
