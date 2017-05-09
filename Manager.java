package prog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Manager {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		//Counters for words
		private int positiveCount;
		private int negativeCount;
		//variable to product ID
		private Text product_id = new Text();

		private Set<String> posWords = new HashSet<String>();
		private Set<String> negWords = new HashSet<String>();

		protected void 
		setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

			URI[] localPaths = context.getCacheFiles();

			parsePositive(localPaths[0]); //get positive words
			parseNegative(localPaths[1]); //get negative words
		}

		// Parse the positive words to match and capture during Map phase.
		private void parsePositive(URI posWordsUri) {
			try {
				BufferedReader rd = new BufferedReader(new FileReader(new File(posWordsUri.getPath()).getName()));
				String posWord;
				while ((posWord = rd.readLine()) != null) {
					posWords.add(posWord);
				}
				rd.close();
			} catch (IOException ioe) {
				System.err.println("Caught ception parsing cached file '" + posWords + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		// Parse the negative words to match and capture during Reduce phase.
		private void parseNegative(URI negWordsUri) {
			try {
				BufferedReader rd = new BufferedReader(new FileReader(new File(negWordsUri.getPath()).getName()));
				String negWord;
				while ((negWord = rd.readLine()) != null) {
					negWords.add(negWord);
				}
				rd.close();
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing cached file '" + negWords + "' : " + StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String[] line = lineText.toString().split("\\t"); //split the file by each tab

			product_id = new Text(line[1]); //gets product ID from split file

			//tokenizer for body with delimiters to remove symbols, punctuation, and returns
			StringTokenizer bodyReader = new StringTokenizer(line[7].toLowerCase(), "\t\r\f\n!@#$%^&*?()'\"[]{}.,/:; ");


			positiveCount = 0;
			negativeCount = 0;
			String word;

			//Count each word in body
			while (bodyReader.hasMoreTokens()) {
				word = bodyReader.nextToken();
				//count positive words
				if (posWords.contains(word))
					positiveCount++;
				//count negative words
				if (negWords.contains(word))
					negativeCount++;
			}

			// 1 for Positive, -1 for Negative, 0 for Neutral, -2 for error.
			if (positiveCount + negativeCount > 0) {         
				if(positiveCount > negativeCount) {
					context.write(product_id, new IntWritable(1));
				}
				else if(positiveCount < negativeCount)
					context.write(product_id, new IntWritable(-1));
				else
					context.write(product_id, new IntWritable(0));
			}
			else
				context.write(product_id, new IntWritable(-2)); 

		}
	}//END OF MAPPER

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> instances, Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;

			for (IntWritable instance : instances) {
				sum += instance.get();
			}

			context.write(word, new IntWritable(sum));
		}
	}//END OF REDUCER

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Manager");
		//Job Methods
		job.setJarByClass(Manager.class);
		//putting each text file into HDFS
		job.addCacheFile(new Path(args[0]).toUri()); //positive
		job.addCacheFile(new Path(args[1]).toUri()); //negative
		FileInputFormat.addInputPath(job, new Path(args[2]));//data
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		int result = job.waitForCompletion(true) ? 0:1;
		System.exit(result);
	}

}
