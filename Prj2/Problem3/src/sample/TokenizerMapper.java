package sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	//private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private static final transient Logger LOG = LoggerFactory.getLogger(TokenizerMapper.class);
    public static List<Integer> allFollowers = new ArrayList<Integer>();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		int c1, c2, c3;
		c1 = 0;
		c2 = 0;
		c3 = 0;
		try{
			Path outFile = new Path("/output" + "/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outFile)));
			String line;
			String newc1, newc2, newc3;
			newc1="";
			newc2="";
			newc3="";
			while((line = br.readLine()) != null)
			{
				String[] vals = line.split("\\s++");
				if(newc1.equals(""))
				{
					newc1 = vals[0];
				}
				else if(newc2.equals("") && (vals[0].equals(newc1) == false))
				{
					newc2 = vals[0];
				}
				else if(newc3.equals("") && (vals[0].equals(newc1) == false) && (vals[0].equals(newc2) == false))
				{
					newc3 = vals[0];
				}	
			}
		
			c1 = Integer.parseInt(newc1);
			c2 = Integer.parseInt(newc2);
			c3 = Integer.parseInt(newc3);
			
		}
		catch(Exception FileNotFoundException)
		{
			//first iteration, initialize centroids
			StringTokenizer itr = new StringTokenizer(value.toString());
			int max = 0;
			int min = 0;
			int flag = 0;
			
			while (itr.hasMoreTokens())
			{
				String strFollowers = itr.nextToken();
				int followers = Integer.parseInt(strFollowers);
				if(flag == 0)
				{
					max = followers;
					min = followers;
					flag = 1;
				}
				else if(followers > max)
				{
					max = followers;
				}
				else if(followers < min)
				{
					min = followers;
				}
				allFollowers.add(followers);
			}
			Random rand = new Random();
			for(int i=0; i<3; i++)
			{
				int centroid = 0;
				if(i==0)
				{
					centroid = rand.nextInt(max) + 1;
					//Centroids.c1 = new Text();
					c1 = centroid;
				}
				if(i==1)
				{
					centroid = rand.nextInt(max) + 1;
					//Centroids.c2 = new Text();
					c2 = centroid;
				}
				if(i==2)
				{
					centroid = rand.nextInt(max) + 1;
					//Centroids.c3 = new Text();
					c3 = centroid;
				}
			}
			Centroids.flag = 1;
		}
		
		//Iterator<Integer> itr = allFollowers.iterator();
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens())
		{
			String followcount = itr.nextToken();
			int follow = Integer.parseInt(followcount);
			IntWritable followers = new IntWritable();
			followers.set(follow);
			int c1diff, c2diff, c3diff;
			LOG.info("MAPPER: c1 = "+c1);
			LOG.info("MAPPER: c2 = "+c2);
			LOG.info("MAPPER: c3 = "+c3);
			c1diff = Math.abs(c1 - follow);
			c2diff = Math.abs(c2 - follow);
			c3diff = Math.abs(c3 - follow);
				
			if(c1diff<=c2diff && c1diff<=c3diff)
			{
				word.set(Integer.toString(c1));
				context.write(word, followers);
			}
			else if(c2diff<=c1diff && c2diff<=c3diff)
			{
				word.set(Integer.toString(c2));
				context.write(word, followers);
			}
			else if(c3diff<=c1diff && c3diff<=c2diff)
			{
				word.set(Integer.toString(c3));
				context.write(word, followers);
			}
		}		
			
	}
		/*StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}*/
}
