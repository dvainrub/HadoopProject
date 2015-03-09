package tweet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONObject;


public class Tweet implements Writable, WritableComparable<Tweet> {
	
	protected TweetKey tweetKey;
	protected TweetValue tweetValue;

	public Tweet(TweetKey tweetKey, TweetValue tweetValue) {
		this.tweetKey = tweetKey;
		this.tweetValue = tweetValue;
	}
	
	public Tweet(){
		tweetKey = new TweetKey();
		tweetValue = new TweetValue();
	}
	

	public Tweet(String jsonString) {
		JSONObject tweetJson = new JSONObject(jsonString);
		tweetKey = new TweetKey(tweetJson.getJSONObject("tweetKey").toString());
		tweetValue = new TweetValue(tweetJson.getJSONObject("tweetValue").toString());
	}
	
	public Tweet(Tweet other) {
		tweetKey = new TweetKey(other.getTweetKey());
		tweetValue = new TweetValue(other.getTweetValue());
	}

	@Override
	public String toString() {
		
		//json.put("tweetKey", tweetKey.toString()).put("tweetValue", tweetValue.toString());
		//json.put("tweet", tweetKey.toString()+tweetValue.toString());
		return getJson().toString();
	}	
	
	public JSONObject getJson(){
		JSONObject json = new JSONObject();
		json.put("tweetKey", tweetKey.getJson()).put("tweetValue", tweetValue.getJson());
		return json;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tweetKey.readFields(in);
		tweetValue.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		tweetKey.write(out);
		tweetValue.write(out);
	}

	public TweetKey getTweetKey() {
		return tweetKey;
	}

	public TweetValue getTweetValue() {
		return tweetValue;
	}

	//TODO esto quizas soluciona el problema de que no llegue los mismos tweets al mismo reducer
	@Override
	public int compareTo(Tweet other) {
		return tweetKey.toString().compareTo(other.tweetKey.toString());
	}

}
