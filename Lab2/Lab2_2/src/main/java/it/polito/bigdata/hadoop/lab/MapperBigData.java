package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends Mapper<Text, // Input key type
		Text, // Input value type
		IntWritable, // Output key type
		IntWritable> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {
		Integer count = Integer.parseInt(value.toString());
		int groupId = 0;
		if(count >= 500) {
			groupId = 5;
		}
		else {
			groupId = (int) Math.floor(count / 100);
		}
		context.write(new IntWritable(groupId), new IntWritable(1));
	}

}
