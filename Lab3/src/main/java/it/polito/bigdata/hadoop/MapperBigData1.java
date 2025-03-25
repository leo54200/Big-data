package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String[] products = value.toString().split(",");
            for (int p1 = 1; p1 < products.length; p1++) {
                for (int p2 = 1; p2 < products.length; p2++) {
                    if (products[p1].compareTo(products[p2])<0) {
                        context.write(new Text(products[p1] + "," + products[p2]), new IntWritable(1));
                    }
                }
            }
    }
}
