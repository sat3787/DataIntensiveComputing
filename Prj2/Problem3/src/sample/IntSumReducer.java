package sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	private static final transient Logger LOG = LoggerFactory.getLogger(IntSumReducer.class);
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		int i = 0;
		for (IntWritable val : values) {
			sum += val.get();
			i = val.get();
			result.set(i);
			context.write(key, result);
			count++;
		}
		//result.set(sum);
		//context.write(key, result);

		/*if(currentCentroid == Centroids.c1)
		{
				Centroids.c1 = centroid;
				//oldCentroids.c2 = new Text();
				oldCentroids.c2 = currentCentroid;
			
		}
		if(currentCentroid == Centroids.c2)
		{
				Centroids.c2 = centroid;
				//oldCentroids.c2 = new Text();
				oldCentroids.c2 = currentCentroid;
		}
		if(currentCentroid == Centroids.c3)
		{
			//if(newCentroid.equals(Centroids.c3) == false)
			//{
				Centroids.c3 = centroid;
				//oldCentroids.c3 = new Text();
				oldCentroids.c3 = currentCentroid;
			//}
			//else
			//{
			//	if(Centroids.c3flag == 0)
			//	{
			//		Centroids.runFlag++;
			//		Centroids.c3flag = 1;
			//	}
		}*/
	}
		
}

