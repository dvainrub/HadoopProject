import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.json.JSONObject;


public class TweetKey implements WritableComparable<TweetKey> {

	protected String createdAt;
	protected long tweetId;

	//TODO leave this??
	/*public TweetKey(String createdAt, long tweetId) {
		this.createdAt = createdAt;
		this.tweetId = tweetId;
	}*/

	public TweetKey(){
		createdAt = "";
		tweetId = 0;
	}

	public TweetKey(String userSerialization, int startIndex) {
		JSONObject tweetJson = new JSONObject(userSerialization);
		createdAt = tweetJson.getString("created_at");
		tweetId = tweetJson.getLong("id");
	}


	public TweetKey(TweetKey other) {
		createdAt = other.createdAt;
		tweetId = other.tweetId;
	}

	public TweetKey(String string) {
		//System.out.println("\n\n"+string+"\n\n");
		String[] s = string.split(TweetMapReduce.TWEET_OR_VALUE_FIELDS_DELIMETER);
		//System.out.println(s[0]+"\n\n");
		createdAt = s[0];
	//	System.out.println(s[1]);
		tweetId = Long.valueOf(s[1]);
	}

	@Override
	public String toString() {
		return createdAt+TweetMapReduce.TWEET_OR_VALUE_FIELDS_DELIMETER+tweetId;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		tweetId = in.readLong();
		createdAt = in.readUTF();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(tweetId);
		out.writeUTF(createdAt);
	}

	@Override
	public int compareTo(TweetKey other) {
		return (int) (tweetId - other.tweetId);
	}
}
