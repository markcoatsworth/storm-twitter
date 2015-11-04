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
public class TwitterKeywordsSpout extends BaseRichSpout {

	int TweetsCollected;
	LinkedBlockingQueue<Status> queue = null;
	SpoutOutputCollector _collector;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;
	TwitterStream _twitterStream;
	
	protected static Logger log = LoggerFactory.getLogger("TwitterKeywordsSpout");

	public TwitterKeywordsSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
		this.TweetsCollected = 0;
		
	}

	public TwitterKeywordsSpout() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				log.info("Adding new tweet to queue");
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

		};

		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);
		_twitterStream = twitterStream;
		
		
		if (keyWords.length == 0) {

			twitterStream.sample();
		}

		else {
			// TODO: Adjust the query below to also track locations and languages.
			FilterQuery query = new FilterQuery();
			query.track(keyWords);
			query.language(new String[]{"en"});
			
			twitterStream.filter(query);
			
			
		}

	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		
		// If we have no tweets (which is very unlikely) then wait another 10 seconds
		if (ret == null) {
			Utils.sleep(50);
		} 
		// Otherwise send them all to the collector
		else {
			try {
				_collector.emit(new Values(ret));
			}
			catch(Exception ex) {
				log.error("Error collecting tweets: " + ex.getMessage());
				ex.printStackTrace();
			}			
		}
	}

	@Override
	public void close() {
		log.info("Closing Twitter stream...");
		_twitterStream.shutdown();
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
