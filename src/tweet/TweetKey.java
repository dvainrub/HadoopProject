package tweet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONObject;


public class TweetKey implements WritableComparable<TweetKey>, TweetInterface {

	protected Text createdAt;
	protected LongWritable tweetId;

	//TODO leave this??
	/*public TweetKey(String createdAt, long tweetId) {
		this.createdAt = createdAt;
		this.tweetId = tweetId;
	}*/

	public TweetKey(){
		createdAt = new Text();
		tweetId = new LongWritable(0);
	}

	public TweetKey(String userSerialization, int startIndex) {
		JSONObject tweetJson = new JSONObject(userSerialization);
		createdAt = new Text (tweetJson.getString("created_at"));
		tweetId = new LongWritable(tweetJson.getLong("id"));
	}
	
	public TweetKey(String jsonString) {
		JSONObject tweetJson = new JSONObject(jsonString);
		createdAt = new Text (tweetJson.getString("createdAt"));
		tweetId = new LongWritable(tweetJson.getLong("tweetId"));
	}



	public TweetKey(TweetKey other) {
		createdAt = other.createdAt;
		tweetId = other.tweetId;
	}

	/*public TweetKey(String string) {
		//System.out.println("\n\n"+string+"\n\n");
		String[] s = string.split(TweetMapReduce.TWEET_OR_VALUE_FIELDS_DELIMETER);
		//System.out.println(s[0]+"\n\n");
		createdAt = new Text(s[0]);
	//	System.out.println(s[1]);
		tweetId = new LongWritable(Long.valueOf(s[1]));
	}*/

	@Override
	public String toString() {
//		JSONObject json = new JSONObject();
//		json.put("createdAt", createdAt.toString()).put("tweetId", tweetId);
		return getJson().toString();
	}
	
	public JSONObject getJson(){
		JSONObject json = new JSONObject();
		json.put("createdAt", createdAt.toString()).put("tweetId", tweetId);
		return json;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		tweetId = new LongWritable(in.readLong());
		createdAt = new Text (in.readUTF());

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(tweetId.get());
		out.writeUTF(createdAt.toString());
	}

	@Override
	public int compareTo(TweetKey other) {
		return (int) (tweetId.get() - other.tweetId.get());
	}
}
