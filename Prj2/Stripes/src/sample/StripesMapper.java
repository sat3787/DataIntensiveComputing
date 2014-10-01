package sample;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable>{
	private MapWritable H =new MapWritable();
	private Text word = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] w =value.toString().split("\\s+");
		if(w.length>1){
			for(int i=0;i<w.length;i++){
				
				H.clear();
				for(int j=0;j<w.length;j++){
					if(j==i) continue;
					Text u=new Text(w[j]);
					if (H.containsKey(u)){
						IntWritable Count= (IntWritable) H.get(u);
						Count.set(Count.get()+1);
					}
					else{
						H.put(u,new IntWritable(1));
					}
				}
				word.set(w[i]);
				context.write(word,H);
			}			
		}		
	}
}