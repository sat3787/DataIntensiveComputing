package sample;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer 
extends Reducer<Text,MapWritable,Text,MapWritable> {
	private MapWritable H_final = new MapWritable();
	

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		H_final.clear();
		for(MapWritable value:values){
			Set<Writable> keys =value.keySet();
			for(Writable keyterm: keys){
				IntWritable fromCount = (IntWritable) value.get(keyterm);
	            if (H_final.containsKey(keyterm)) {
	                IntWritable count = (IntWritable) H_final.get(keyterm);
	                count.set(count.get() + fromCount.get());
	            } else {
	                H_final.put(keyterm, fromCount);
	            }
			}
		}
				
		context.write(key,H_final);
	}
}
