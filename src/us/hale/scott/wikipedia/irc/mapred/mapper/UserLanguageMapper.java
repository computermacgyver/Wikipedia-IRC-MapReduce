package us.hale.scott.wikipedia.irc.mapred.mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import us.hale.scott.wikipedia.irc.model.RecentChange;

public class UserLanguageMapper extends Mapper<Text, Text, Text, Text> {

	private Text outKey = new Text("*");
	private Text val = new Text();
	
	private static Vector<String> localAccounts;
	static {
		localAccounts = new Vector<String>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("localAccounts.txt"));
			String line;
			while ((line=br.readLine())!=null) {
				localAccounts.add(line);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

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
			
			//Include Simple English or not
			/*if ("#simple.wikipedia".equals(rc.getChannel())) {
				return;
				//rc.setChannel("#en.wikipedia");
			}*/
			
			//If the user account is a local account
			//(from having consulted the Central Authorization database)
			//then prepend the language edition and a colon to the username to avoid 
			if (localAccounts.contains("*:"+rc.getUser()) ||
					localAccounts.contains(rc.getLanguage()+":"+rc.getUser())) {
				rc.setUser(rc.getLanguage()+":"+rc.getUser());
			}
					

			String username = rc.getUser();	
			
			outKey.set(username);
			//val.set(rc.getLanguage());
			val.set(rc.toJson().toString());
			context.write(outKey, val);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
	