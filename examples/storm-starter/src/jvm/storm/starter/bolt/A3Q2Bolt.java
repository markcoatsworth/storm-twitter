package storm.starter.bolt;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class A3Q2Bolt extends BaseBasicBolt {

	protected int intervalTimeSecs = 3;
	protected int numTimedIntervals = 200;
	protected long intervalStartTime;
	protected static Logger log = LoggerFactory.getLogger("A3Bolt");
	protected ArrayList<String> hashTags;
	protected ArrayList<Status> tweets;
	
	public A3Q2Bolt() {
		hashTags = new ArrayList<String>();
		tweets = new ArrayList<Status>();
		intervalStartTime = System.currentTimeMillis();
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//log.info("Called execute! input.getSourceComponent()=" + input.getSourceComponent());
		
		// When we receive a tweet, add it to in-memory list
		if(input.getSourceComponent().equals("twitter")) {
			for(Object inputValue : input.getValues()) {
				Status thisTweet = (Status) inputValue;
				//log.info("Tweet text: " + tweet.getText().replaceAll("(\\r|\\n)", ""));
				tweets.add(thisTweet);
			}
		}
		// When we receive a hashtag, add it to the in-memory list (if it doesn't exist there already)
		else if(input.getSourceComponent().equals("hashtags")) {
			for(Object inputValue : input.getValues()) {
				String thisHashtag = inputValue.toString();
				if(!hashTags.contains(thisHashtag)) {
					//log.info("Adding hashtag " + thisHashtag + " to in-memory list");
					hashTags.add(thisHashtag);
				}
			}
		}
		
		// If we have exceeded the interval time, process the data currently being stored
		if(System.currentTimeMillis() > (intervalStartTime + (1000 * intervalTimeSecs))) {
		
			log.info("End of time interval!");
			if(tweets != null) {
				log.info("Tweets count=" + tweets.size());
			}
			if(hashTags != null) {
				log.info("Hashtags count=" + hashTags.size());
			}
			
			// Reset the data collection lists & interval start time
			tweets.clear();
			hashTags.clear();
			intervalStartTime = System.currentTimeMillis();
		}
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}

/*
Fields inputFields = input.getFields(); // input.getFields();
for(int i = 0; i < inputFields.size(); i ++) {
	if(inputFields.get(i).toString().equals("tweet")) {
		//log.info("Tweet! field.toString()=" + input.getFields().get(0).toString());
	}
	//log.info("input.fields[" + i + "]=" + inputFields.get(i).toString() );
}
*/
//log.info("input.getValue()=" + input.getValue());
//log.info("input=" + input.toString());

//log.info("collector=" + collector.
//log.info("input.getSourceComponent()=" + input.getSourceComponent());
