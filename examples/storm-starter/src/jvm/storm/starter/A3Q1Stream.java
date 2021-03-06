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

package storm.starter;

import java.util.Arrays;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.TopologySummary;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.starter.bolt.*;
import storm.starter.spout.*;

public class A3Q1Stream {    
	
	protected static Logger log = LoggerFactory.getLogger("A3Q1NoKeywordsStream");
	
    public static void main(String[] args) {
        String consumerKey = args[0]; 
        String consumerSecret = args[1]; 
        String accessToken = args[2]; 
        String accessTokenSecret = args[3];
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        
        // Set up + configure the topology (job)
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterKeywordsSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));
        
        builder.setBolt("fileoutput", new FileOutputBolt())
        	.shuffleGrouping("twitter");
                
        // Send the topology to the compute cluster        
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        
        
        try {
        	// Sleep for a long time
	        Utils.sleep(7200000);
	        
	        // Now shut it down!
	        cluster.shutdown();
        }
        catch(Exception ex) {
        	log.error("Failed to read topology data.");
        	ex.printStackTrace();
        }
    }
}
