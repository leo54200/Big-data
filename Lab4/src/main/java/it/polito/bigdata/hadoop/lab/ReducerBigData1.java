package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        List<Text> valuesList = new ArrayList<>();
        int score;
        int tot_score = 0;
        double average;
        int count = 0;
        for(Text value: values){
            valuesList.add(value);
            score = Integer.parseInt(value.toString().split("_")[1]);
            tot_score = tot_score + score;
            count++;
        }
        average = tot_score/count;
        for(Text value: valuesList){
            score = Integer.parseInt(value.toString().split("_")[1]);
            String productId = value.toString().split("_")[0];
        context.write(new Text(productId), new DoubleWritable(score - average));
        }

		/* Implement the reduce method */
    	
    }
}
