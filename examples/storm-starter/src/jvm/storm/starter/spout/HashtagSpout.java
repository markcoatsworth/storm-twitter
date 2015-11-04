/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter.spout;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class HashtagSpout extends BaseRichSpout {

	int hashTagsPerInterval = 3;
	int intervalLengthSecs = 60;
	LinkedBlockingQueue<String> queue = null;
	SpoutOutputCollector _collector;
	String[] hashTags;
	TwitterStream _twitterStream;
	
	protected static Logger log = LoggerFactory.getLogger("HashtagSpout");

	public HashtagSpout(String[] hashTags) {
		this.hashTags = hashTags;
	}

	public HashtagSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		log.info("Called open...");
		
		Random randomGenerator = new Random();
		queue = new LinkedBlockingQueue<String>(1000);
		_collector = collector;
		
		// Every time interval, send a random hashtag to the bolt. 
		while(true) {
			
			int randIndex = randomGenerator.nextInt(hashTags.length);
			queue.offer(hashTags[randIndex]);
			
			// Send to the collector
			_collector.emit(new Values(queue.poll()));
			
			// Wait...
			Utils.sleep((intervalLengthSecs / hashTagsPerInterval) * 1000);
		}		
	}

	@Override
	public void nextTuple() {
		
		log.info("nextTuple called!");
		
		/*Utils.sleep(5000);
		
		String hashtag = queue.poll();
		if (hashtag == null) {
			Utils.sleep(1000);
		} 
		else {
			try {
				log.info("Got something! Emitting to collector...");
				_collector.emit(new Values(hashtag));
			}
			catch(Exception ex) {
				log.error("Could not write result: " + ex.getMessage());
				ex.printStackTrace();
			}			
		}
		*/
	}

	@Override
	public void close() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
