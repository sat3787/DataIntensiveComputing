import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class Sorter {
	
	public static void main(String[] args) throws IOException
	{
		FileReader fr = new FileReader("/Users/frankrossi/Downloads/part-r-00000"); //Replace with path to output file
		BufferedReader br = new BufferedReader(fr);
		
		int hashMax = 0;
		int ampMax = 0;
		int totalHash = 0;
		int totalAmp = 0;
		
		String mostHashed = "";
		String mostAt = "";
		String line;
		
		while((line = br.readLine()) != null)
		{
			String[] vals = line.split("\\s++");
			String word = vals[0];
			String strCount = vals[1];
			int count = Integer.parseInt(strCount);
			String type = word.substring(0, 1);
			
			if(type.equals("#"))
			{
				totalHash++;
				if(count > hashMax)
				{
					hashMax = count;
					mostHashed = word;
				}
			}
			else if(type.equals("@"))
			{
				totalAmp++;
				if(count > ampMax)
				{
					if(word.equals("@") != true)
					{	
						ampMax = count;
						mostAt = word;
					}
					
				}
			}
		}
		System.out.println("Most Trending: " + mostHashed + " " + hashMax);
		System.out.println("Most tweeted at: " + mostAt + " " + ampMax);
		System.out.println("Total #tag count: " + totalHash);
		System.out.println("Total @xyz count: " + totalAmp);
	}

}
