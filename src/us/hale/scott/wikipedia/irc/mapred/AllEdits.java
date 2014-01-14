package us.hale.scott.wikipedia.irc.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import us.hale.scott.wikipedia.irc.mapred.mapper.AllEditsMapper;

public class AllEdits {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wikiAllEdits");
		job.setJarByClass(AllEdits.class);
		job.setMapperClass(AllEditsMapper.class);
		job.setReducerClass(us.hale.scott.mapred.reducer.DedupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job,new Path("/output/from/logger/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output_allEdits_noMinor_noBots_noSimple"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
