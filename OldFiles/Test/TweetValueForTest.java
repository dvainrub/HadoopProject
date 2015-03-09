package Test;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class TweetValueForTest implements Writable{

	protected String username;

	public TweetValueForTest(){
		username = null;

	}
	
	public TweetValueForTest(String username) {
		this.username = username;
	}


	public TweetValueForTest(String userSerialization, int startIndex) {
		
		username = "userSerialization: "+userSerialization+" startIndex: " +startIndex;
    }




	@Override
	public String toString() {
		
		return "Username: " + username;
	}




	@Override
	public void readFields(DataInput in) throws IOException {

		username = in.readUTF();

		
	}

	@Override
	public void write(DataOutput out) throws IOException {

        out.writeUTF(username);

	}
	


	
}
