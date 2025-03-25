package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type
    
    private TopKVector<WordCountWritable> localTopK;
    private Integer k = 100;

    protected void setup(Context context) {
        localTopK = new TopKVector<WordCountWritable>(k);
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            WordCountWritable currentPair = new WordCountWritable(key.toString(),
            Integer.parseInt(value.toString()));
            localTopK.updateWithNewElement(currentPair);
            }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // emit the local top-k list
        for (WordCountWritable p : localTopK.getLocalTopK()) {
            context.write(NullWritable.get(), new WordCountWritable(p));
        }
    }
}
