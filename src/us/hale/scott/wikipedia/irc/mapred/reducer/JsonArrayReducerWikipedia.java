package us.hale.scott.wikipedia.irc.mapred.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class JsonArrayReducerWikipedia extends Reducer<Text, Text, Text, Text> {
	
	private static Logger logger = LoggerFactory.getLogger(JsonArrayReducerWikipedia.class);

	private ArrayList<JSONObject> edits = new ArrayList<JSONObject>();
	private HashMap<String,Integer> langCounts = new HashMap<String,Integer>();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		edits.clear();
		langCounts.clear();
		
		for (Text str : values) {
			try {
				edits.add(new JSONObject(str.toString()));
			} catch (JSONException e) {
				logger.error("Error parsing JSON from mapper in reducer.",e);
			}
		}
		
		String majLang="";
		int majLangCount=0;
		
		for (JSONObject json : edits) {
			String lang;
			try {
				lang = json.getString("language");
				Integer count = langCounts.get(lang);
				if (count==null) {
					count=Integer.valueOf(1);
				} else {
					count=Integer.valueOf(count.intValue()+1);
				}
				
				langCounts.put(lang,count);
				
				if (count.intValue()>majLangCount) {
					majLangCount=count.intValue();
					majLang=lang;
				}
			} catch (JSONException e) {
				logger.warn("Language key not found",e);
			}
			
		}
		
		double avgSize=0;
		Set<JSONObject> unqEdits = new HashSet<JSONObject>(edits);
		for (JSONObject edit : unqEdits) {
			try {
				String size = edit.getString("lineCount");
				avgSize+=Integer.valueOf(size);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		avgSize=avgSize/unqEdits.size();
		
		JSONArray json = new JSONArray(unqEdits);
		outVal.set(json.toString()
				+"\t"+unqEdits.size()
				+"\t"+avgSize
				+"\t"+majLang
				+"\t"+majLangCount
				+"\t"+langCounts.size()
				);
		context.write(key, outVal);
	}
}