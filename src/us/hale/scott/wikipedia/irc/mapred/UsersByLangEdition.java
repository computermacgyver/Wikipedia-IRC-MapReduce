package us.hale.scott.wikipedia.irc.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import us.hale.scott.wikipedia.irc.mapred.mapper.UserLanguageMapper;
import us.hale.scott.wikipedia.irc.mapred.reducer.JsonArrayReducerWikipedia;

public class UsersByLangEdition {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wikiLanguage");
		job.setJarByClass(UsersByLangEdition.class);
		job.setMapperClass(UserLanguageMapper.class);
		job.setReducerClass(JsonArrayReducerWikipedia.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job,new Path("/output/from/logger/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output_wikiLangs_noMinor_noBots_withSimple_filter"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
