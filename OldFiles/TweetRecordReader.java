import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
 
 
public class TweetRecordReader extends RecordReader<TweetKey,TweetValue> { 
 
    LineRecordReader reader;
    
    TweetRecordReader() {
        reader = new LineRecordReader(); 
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }
 
 
    @Override
    public void close() throws IOException {
        reader.close();        
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }
    
    @Override
    public TweetKey getCurrentKey() throws IOException, InterruptedException {
        return new TweetKey(reader.getCurrentValue().toString(),0);
    }
    
    @Override
    public TweetValue getCurrentValue() throws IOException, InterruptedException {
        return new TweetValue(reader.getCurrentValue().toString(),0);    
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
    
    
 
    
}