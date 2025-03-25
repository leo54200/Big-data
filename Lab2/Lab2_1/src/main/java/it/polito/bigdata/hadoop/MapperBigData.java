package it.polito.bigdata.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    String prefix;
    
    protected void setup(Context context) {
        prefix = context.getConfiguration().get("prefix").toString();
    }
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            if(key.toString().startsWith(prefix)) {
                context.write(key, value);
            }
    }
}
