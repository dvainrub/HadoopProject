package Test;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class TweetKeyForTest implements WritableComparable<TweetKeyForTest> {

	protected String createdAt;


	public TweetKeyForTest(String createdAt) {
		this.createdAt = createdAt;
	}

	public TweetKeyForTest(){
		createdAt = null;
	}

	public TweetKeyForTest(String userSerialization, int startIndex) {
		createdAt = userSerialization;
	}


	@Override
	public String toString() {
		return "Created at: " +createdAt;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		createdAt = in.readUTF();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(createdAt);
	}

	@Override
	public int compareTo(TweetKeyForTest other) {
		return (int) (createdAt.hashCode() - other.createdAt.hashCode());
	}
}
