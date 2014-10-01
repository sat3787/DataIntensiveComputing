package sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

	private static final transient Logger LOG = LoggerFactory.getLogger(WordCount.class);
   
    //public static Centroids newCentroids = new Centroids();
    //public static Centroids oldCentroids = new Centroids();
	public static void main(String[] args) throws Exception {
		
		Centroids.flag = 1;
		Centroids.runFlag = 0;
		Centroids.c1flag = 0;
		Centroids.c2flag = 0;
		Centroids.c3flag = 0;
		int iteration = 0;
		
		//String testCent = "10";
		//Centroids.c1.set(testCent);
		//Centroids.c2.set(testCent);
		//Centroids.c3.set(testCent);
		
		boolean runflag = true;
		while(runflag == true){
		iteration++;
		Configuration conf = new Configuration();		

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/output";


		String line, oldc1, oldc2, oldc3, newc1, newc2, newc3;
		oldc1 = "";
		oldc2 = "";
		oldc3 = "";
		newc1 = "";
		newc2 = "";
		newc3 = "";
		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */
		
		try{
			Path outFile = new Path(outputPath + "/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outFile)));
			
			while((line = br.readLine()) != null)
			{
				String[] vals = line.split("\\s++");
				if(oldc1.equals(""))
				{
					oldc1 = vals[0];
				}
				else if(oldc2.equals("") && (vals[0].equals(oldc1) == false))
				{
					oldc2 = vals[0];
				}
				else if(oldc3.equals("") && (vals[0].equals(oldc1) == false) && (vals[0].equals(oldc2) == false))
				{
					oldc3 = vals[0];
				}	
			}
		}
		catch(Exception FileNotFoundException)
		{
			//do nothing
		}
		
		
		
		deleteFolder(conf,outputPath);
		Job job = Job.getInstance(conf);

		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		try{
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
		}
		catch(Exception FileAlreadyExistsException)
		{
			//do nothing
		}
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);
		
		Path outFile = new Path(outputPath + "/part-r-00000");
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outFile)));
		
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
		int oc1, oc2, oc3, nc1, nc2, nc3;
		oc1 = 0;
		oc2 = 0;
		oc3 = 0;
		try{
			oc1 = Integer.parseInt(oldc1);
			oc2 = Integer.parseInt(oldc2);
			oc3 = Integer.parseInt(oldc3);
		}
		catch(Exception NumberFormatException)
		{
			//do nothing
		}
		nc1 = Integer.parseInt(newc1);
		nc2 = Integer.parseInt(newc2);
		nc3 = Integer.parseInt(newc3);
		LOG.info("OLD CENTROID:" +oc1+" "+oc2+" "+oc3);
		LOG.info("NEW CENTROIDS:" +nc1+" "+nc2+" "+nc3);
		if(oc1 == nc1 && oc2 == nc2 && oc3 == nc3)
		{
			runflag = false;
		}
		
	}
}		
	
	/**
	 * Delete a folder on the HDFS. This is an example of how to interact
	 * with the HDFS using the Java API. You can also interact with it
	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf a Hadoop Configuration object
	 * @param folderPath folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}