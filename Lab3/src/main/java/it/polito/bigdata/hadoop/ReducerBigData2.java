package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    private TopKVector<WordCountWritable> globalTopK;
    private Integer k = 100;

    protected void setup(Context context) {
        globalTopK = new TopKVector<WordCountWritable>(k);
    }
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
		// Each value in values is a pair of products + its count
		for (WordCountWritable currentPair : values) {
			// Update the current top k list with the current pair
			globalTopK.updateWithNewElement(new WordCountWritable(currentPair));
		}
        
		// Emit the global top k list
		for (WordCountWritable p : globalTopK.getLocalTopK()) {
			context.write(new Text(p.getWord()), new IntWritable(p.getCount()));
		}
    }
}
