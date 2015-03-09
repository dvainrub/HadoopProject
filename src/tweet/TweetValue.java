package tweet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.json.JSONObject;


public class TweetValue implements TweetInterface{

	protected String user;
	protected String text;
	protected boolean favorited;
	protected boolean retweeted;
	//protected int num_of_words;
	//protected MapWritable tweets_words; // <wored, counter>
	;


	public TweetValue(){
		user = "";
		text ="";
		favorited = false;
		retweeted = false;
	//	tweets_words = new MapWritable();
		//num_of_words = 0;
	}

	/*public TweetValue(String username, String text, boolean favorited,
			boolean retweeted) {
		this.user = username.replaceAll("\t", TweetMapReduce.TAB_DELIMITER);
		this.text = text.replaceAll("\n", TweetMapReduce.SLASH_N_DELIMETER).replaceAll("\t", TweetMapReduce.TAB_DELIMITER);
		this.favorited = favorited;
		this.retweeted = retweeted;
		//num_of_words = 0;
		//tweets_words = new MapWritable();
		//count_words(Filter.filter_stop_words(text)); // Initialize the tweets_words
	}*/

	public TweetValue(String username, String text, boolean favorited,
			boolean retweeted) {
		this.user = replaceString(username);
		this.text = replaceString(text);
		this.favorited = favorited;
		this.retweeted = retweeted;
		//num_of_words = 0;
		//tweets_words = new MapWritable();
		//count_words(Filter.filter_stop_words(text)); // Initialize the tweets_words
	}

	private String replaceString(String string) {
		String str = string.replaceAll("\t", StaticVars.TAB_DELIMITER)
				.replaceAll("\n\n", StaticVars.SLASH_N_DELIMETER+StaticVars.SLASH_N_DELIMETER)
				.replaceAll("\n", StaticVars.SLASH_N_DELIMETER)
				.replaceAll("  ", " ");
		return str;
	}

	/*public TweetValue(String userSerialization, int startIndex) {
		JSONObject tweetJson = new JSONObject(userSerialization);
		username = tweetJson.getJSONObject("user").getString("name").replaceAll("\t", TweetMapReduce.TAB_DELIMITER);
		text = tweetJson.getString("text").replaceAll("\n", TweetMapReduce.SLASH_N_DELIMETER)
				.replaceAll("\t", TweetMapReduce.TAB_DELIMITER);
		favorited = tweetJson.getBoolean("favorited");
		retweeted = tweetJson.getBoolean("retweeted");
	}*/
	public TweetValue(String userSerialization, int startIndex) {
		JSONObject tweetJson = new JSONObject(userSerialization);
		user = tweetJson.getJSONObject("user").getString("name");
		text = tweetJson.getString("text");
		favorited = tweetJson.getBoolean("favorited");
		retweeted = tweetJson.getBoolean("retweeted");
	}
	
	public TweetValue(String jsonString) {
		JSONObject tweetJson = new JSONObject(jsonString);
		user = tweetJson.getString("user");
		text = tweetJson.getString("text");
		favorited = tweetJson.getBoolean("favorited");
		retweeted = tweetJson.getBoolean("retweeted");
	}




	public TweetValue(TweetValue other) {
		this.user = other.user;
		this.text = other.text;
		this.favorited = other.favorited;
		this.retweeted = other.retweeted;
	}

	/*public TweetValue(String string) {
		String[] s = string.split(TweetMapReduce.TWEET_OR_VALUE_FIELDS_DELIMETER);
		username = s[0].replaceAll("\t", TweetMapReduce.TAB_DELIMITER);;
		text = s[1].replaceAll("\n", TweetMapReduce.SLASH_N_DELIMETER).replaceAll("\t", TweetMapReduce.TAB_DELIMITER);
		if(s.length!=4)
		{
			this.favorited = false;
			this.retweeted = false;
		}
		else
		{
			this.favorited = Boolean.valueOf(s[2]);
			this.retweeted = Boolean.valueOf(s[3]);
		}
	}*/
	
	

	@Override
	/*public String toString() {

		return "Username: " + username +
				"\nText: "+ text +
				"\nFavorited? " + favorited +
				"\nRetweeted? " + retweeted;
				//"\nnum_of_words:" + num_of_words +"\n";
				//"\n\n tweets_words:\n\t" + tweets_words.toString();
	}*/

	public String toString() {
//		JSONObject json = new JSONObject();
//		json.put("user", user).put("text", text).put("favorited", favorited).put("retweeted", retweeted);
		return getJson().toString();
	}
	
	public JSONObject getJson(){
		JSONObject json = new JSONObject();
		json.put("user", user).put("text", text).put("favorited", favorited).put("retweeted", retweeted);
		return json;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		text = in.readUTF();
		user = in.readUTF();
		favorited = in.readBoolean();
		retweeted = in.readBoolean();
	//	num_of_words = in.readInt();
		//tweets_words.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(text);
		out.writeUTF(user);
		out.writeBoolean(favorited);
		out.writeBoolean(retweeted);
		//out.writeInt(num_of_words);
		//tweets_words.write(out);
	}
	
	public Text getTweetText()
	{
		return new Text(text);
	}
	
	
	public double calc_tf(String term) {
		String filteredWords = StaticVars.filterStopWords(text);
		String[] separatedWords = filteredWords.split(" ");
		
		double numOfTimesTermAppearInTweet = 0;
		for (String string : separatedWords)
		{
			if (string.equals(term))
			{
				numOfTimesTermAppearInTweet++;
			}
		}
		return numOfTimesTermAppearInTweet/separatedWords.length;
	}




	/*private void count_words(Text text) 
	{
		String[] words = text.split(" ");
		IntWritable counter;
		Text word;
		num_of_words = words.length;
		for (int i = 0; i < words.length; i++) {
			word = new Text(words[i]);
			if (!tweets_words.containsKey(word))
			{
				tweets_words.put(new Text(word), new IntWritable(1)); // if not exist put 1
			}
			else
			{
				counter = (IntWritable) tweets_words.get(word);
				tweets_words.put(new Text(word), counter); // if exist add 1
				counter.set(counter.get() + 1);
			}
		}

	}*/

}
