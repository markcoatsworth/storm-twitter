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
package storm.starter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;



public class FileOutputBolt extends BaseBasicBolt {

	protected static Logger log = LoggerFactory.getLogger("FileOutputBolt");
	
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
	  
	  try {
		  Status thisTweet = (Status) input.getValues().get(0);
		  
		  if(!thisTweet.getLang().equals("en")) {
			  return;
		  }
		  
		  BufferedWriter ResultsFileWriter = new BufferedWriter(new FileWriter("q1-random-english-tweets.txt", true));
		  String tweetText = thisTweet.getText().replaceAll("(\\r|\\n)", "");
		  log.info("Lang (" + thisTweet.getLang() + "): " + tweetText);
		  ResultsFileWriter.append(tweetText + "\n");
		  ResultsFileWriter.close();
	  }
	  catch(Exception ex) {
		  System.out.println("FileOutputBolt error: " + ex.getMessage());
	  }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }

}
