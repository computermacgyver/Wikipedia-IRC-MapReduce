package us.hale.scott.wikipedia.irc.mapred.mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import us.hale.scott.wikipedia.irc.model.RecentChange;

public class AllEditsMapper extends Mapper<Text, Text, Text, Text> {

	private Text outKey = new Text("*");

	private Text val = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		try {
			if (!value.toString().contains("PRIVMSG #")) {
				return;
			}
			
			RecentChange rc = RecentChange.parse(key.toString()+"\t"+value.toString());

			//Don't incude anonymous edits, bots, minor edits, edits to non-articles			
			if (rc.isAnonymous() || rc.isBot() || rc.getNamespace()!=null || rc.isMinor()) {
				return;
			}

			//Don't include Simple English
			if ("#simple.wikipedia".equals(rc.getChannel())) {
				return;
			}

			outKey.set(rc.toJson().toString());
			context.write(outKey, val);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
	