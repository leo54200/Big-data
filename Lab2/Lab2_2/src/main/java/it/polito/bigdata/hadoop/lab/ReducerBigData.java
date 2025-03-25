package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData extends Reducer<
                                            IntWritable,
                                            IntWritable,
                                            Text,
                                            IntWritable> {
    @Override
    protected void reduce(
        IntWritable key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int count = 0;
    	for (IntWritable val : values)
    		count += val.get();
    	
    	context.write(new Text("Group" + key.toString()), new IntWritable(count));
    	
    }
}
