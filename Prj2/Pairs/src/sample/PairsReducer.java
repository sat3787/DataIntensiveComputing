package sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	//private Text flag = new Text("*");
	//private Text currentWord = new Text();
	//private IntWritable totalCount = new IntWritable();
	//private IntWritable relativeCount = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
		/*String[] inp_Word=key.toString().split("\\,");
		if(inp_Word[1].equals(flag)){
			currentWord.set(inp_Word[0]);
			totalCount.set(0);
			totalCount.set(getTotalCount(values));
			}
		else{
			int count=getTotalCount(values);
			relativeCount.set(count/totalCount.get());
			context.write(key, relativeCount);
		}
	}
	
	private int getTotalCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        return count;*/
    }
}
