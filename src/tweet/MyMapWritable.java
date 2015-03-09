package tweet;


import org.apache.hadoop.io.MapWritable;

public class MyMapWritable extends MapWritable {
	
	@Override
	public String toString() {
		return this.values().toString();
	}

	public int length() {
		return this.values().size();
	}

}
