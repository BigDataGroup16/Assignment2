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
			Path path = new Path ("people.txt");
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			////			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			//////			ClassLoader cl = GetArticlesMapred.class.getClassLoader();
			//////			System.out.println(cl.getResource("people.txt").getFile());
			//BufferedReader br = new BufferedReader(new FileReader(new File("inu/people.txt")));
			////			Path[] localPaths = context.getLocalCacheFiles();
			////			System.out.println(Arrays.toString(localPaths));
			////			LOG.info("vvvv"+Arrays.toString(localPaths));
			//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/peo.txt"))));
			//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("/tmp/hadoop-tmp/hadoop-unjar8883251350104763136/people.txt"))));
			String person;
			while((person = br.readLine()) != null)
				peopleArticlesTitles.add(person);
			br.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {
			//System.out.println(offset+": "+inputPage.getTitle());
			//			Text title = new Text();
			//			Text content = new Text();		
			if(peopleArticlesTitles.contains(inputPage.getTitle())) {
				//				title.set(inputPage.getTitle());
				//				content.set(inputPage.getContent());
				context.write(new Text(""), new Text(inputPage.getContent()));
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.setBoolean("mapreduce.map.output.compress", true);//had been having problems with amount of memory.  Got heap space errors
		//conf.setInt("mapred.map.memory.mb", 4096);//had been having problems with amount of memory.  Got heap space errors
		GenericOptionsParser gop = new GenericOptionsParser(conf, args);
		//		if(otherArgs.length != 2) {
		//			System.err.println("Usage: wordcount <in> <out>");
		//			System.exit(2);
		//		}
		//		URL[] libJars = GenericOptionsParser.getLibJars(conf);
		//		System.out.println(Arrays.toString(libJars));

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "word count");
		job.setJarByClass(GetArticlesMapred.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(GetArticlesMapper.class);
		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(WikipediaPage.class);
		System.out.println("input: "+new Path(args[1]).suffix("/input"));
		System.out.println("output: "+new Path(args[2]));
		//job.addCacheFile(new URI("people.txt"));
		job.addCacheFile(new Path("inu/people.txt").toUri());
		FileInputFormat.addInputPath(job, new Path(args[1]));//.getParent().getParent().getParent().suffix("input"));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
