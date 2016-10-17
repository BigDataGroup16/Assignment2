package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
			ClassLoader cl = GetArticlesMapred.class.getClassLoader();
			BufferedReader br = new BufferedReader(new FileReader(new File(cl.getResource("people.txt").getFile())));
			String person;
			while((person = br.readLine()) != null)
				peopleArticlesTitles.add(person);
			br.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			Text title = new Text();
			Text content = new Text();		
			if(peopleArticlesTitles.contains(inputPage.getTitle())) {
				title.set(inputPage.getTitle());
				content.set(inputPage.getContent());
				context.write(title, content);
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.output.compress", true);//had been having problems with amount of memory.  Got heap space errors
		conf.setInt("mapred.map.memory.mb", 4096);//had been having problems with amount of memory.  Got heap space errors
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "word count");
		job.setJarByClass(GetArticlesMapred.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(GetArticlesMapper.class);
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(WikipediaPage.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
