package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import util.WikipediaPageInputFormat;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {
	//@formatter:off
	/**
	 * Input:
	 * 		Page offset 	WikipediaPage
	 * Output
	 * 		Page offset 	WikipediaPage
	 * @author Tuan
	 *
	 */
	//@formatter:on
	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Path peoplePath = new Path ("people.txt");
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(peoplePath)));//creates a buffered reader for the people.txt file
			String person;
			while((person = br.readLine()) != null)
				peopleArticlesTitles.add(person);
			br.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			if(peopleArticlesTitles.contains(inputPage.getTitle()))
				context.write(new Text(""), new Text(inputPage.getRawXML()));//writes the raw XML of articles of people in the people.txt file
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "word count");
		job.setJarByClass(GetArticlesMapred.class);
		job.setNumReduceTasks(0);//we are not using a reducer for this part so we set the number of reduce tasks to 0.
		job.setMapperClass(GetArticlesMapper.class);
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(WikipediaPage.class);
		job.addCacheFile(new Path("inu/people.txt").toUri());//adds people.txt to cache
		FileInputFormat.addInputPath(job, new Path(args[1]));//sets input path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));//sets output path
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
