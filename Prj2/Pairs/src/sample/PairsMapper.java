package sample;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairsMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	//private IntWritable totalCount=new IntWritable();
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens=value.toString().split("\\s+");
	    if(tokens.length >1) {	
	    for(int i=0;i<tokens.length;i++){
	    	if(tokens[i].equals("")){
	    		continue;
	    	}
	    		String Text1=tokens[i];
	    		for(int j=0;j<tokens.length;j++){
	    			if(j==i)continue;
	    			String Text2= tokens[j];
	    			String Output = Text1 + ','+ Text2;
	    			word.set(Output);
	    			context.write(word, one);
	    	}
	    	//String Output1=Text1+ ',' + '*';
	    	//word.set(Output1);
	    	//totalCount.set(tokens.length-1);
            //context.write(word, totalCount);
	    	
	    }
	}
	}
}