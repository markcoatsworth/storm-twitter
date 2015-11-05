package storm.starter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.WordUtils;
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

	protected int intervalCount = 1;
	protected int intervalTimeSecs = 30;
	protected int numTimedIntervals = 200;
	protected long intervalStartTime;
	protected static Logger log = LoggerFactory.getLogger("A3Q2Bolt");
	protected ArrayList<Integer> randomNumbers;
	protected ArrayList<String> hashTags;
	protected ArrayList<Status> tweets;
	
	public A3Q2Bolt() {
		randomNumbers = new ArrayList<Integer>();
		hashTags = new ArrayList<String>();
		tweets = new ArrayList<Status>();
		intervalStartTime = System.currentTimeMillis();
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		// When we receive a tweet, add it to in-memory list
		if(input.getSourceComponent().equals("twitter")) {
			for(Object inputValue : input.getValues()) {
				Status thisTweet = (Status) inputValue;
				tweets.add(thisTweet);
			}
		}
		// When we receive a hashtag, add it to the in-memory list (if it doesn't exist there already)
		else if(input.getSourceComponent().equals("hashtags")) {
			for(Object inputValue : input.getValues()) {
				String thisHashtag = inputValue.toString();
				if(!hashTags.contains(thisHashtag)) {
					hashTags.add(thisHashtag);
				}
			}
		}
		// When we receive a continent, add it to the in-memory list (if it doesn't exist there already)
		else if(input.getSourceComponent().equals("randomNumbers")) {
			for(Object inputValue : input.getValues()) {
				int thisNumber = (Integer)inputValue;
				if(!randomNumbers.contains(thisNumber)) {
					randomNumbers.add(thisNumber);
				}
			}
		}
		
		// Once we exceed the interval time, process the data currently being stored
		if(System.currentTimeMillis() > (intervalStartTime + (1000 * intervalTimeSecs))) {
		
			log.info("End of time interval! tweets=" + tweets.size() + ", hashtags=" + hashTags.size() + ", randomNumbers=" + randomNumbers.size());
			
			ArrayList<Status> matchingTweets;
			matchingTweets = getMatchingTweets(tweets, hashTags, randomNumbers);
			
			// Output the full list of matching tweets to file
			try {
				outputTweetsToFile(matchingTweets, "q2-output/interval-" + String.format("%03d", intervalCount) + "-tweets.txt");
			}
			catch(Exception ex) {
				log.error("Error outputting tweet to file: " + ex.getMessage());
			}
			
			// Out the top words list to file
			try {
				outputTopWordsToFile(matchingTweets, "q2-output/interval-" + String.format("%03d", intervalCount) + "-words.txt");
			}
			catch(Exception ex) {
				log.error("Error outputting word counts to file: " + ex.getMessage());
			}
			
			intervalCount ++;
			
			// Reset the data collection lists & interval start time
			tweets.clear();
			hashTags.clear();
			randomNumbers.clear();
			intervalStartTime = System.currentTimeMillis();
		}
	}
	
	/*
	 * Returns a list of tweets from sourceTweets that match the criteria in matchHashTags and matchContinents
	 */
	public ArrayList<Status> getMatchingTweets(ArrayList<Status> sourceTweets, ArrayList<String> matchHashTags, ArrayList<Integer> matchRandomNumbers) {
		
		ArrayList<Status> matchingTweets = new ArrayList<Status>();
		boolean isMatching;
		
		for(Status thisTweet : sourceTweets) {
			
			isMatching = false;
			
			// First check against hashtags
			for(String thisHashTag : matchHashTags) {
				if(thisTweet.getText().contains(thisHashTag)) {		
					isMatching = true;
				}
			}
			// Now check against the current random number. Tweeter must have more followers than this number.
			if(randomNumbers.size() > 0) {
				if(thisTweet.getUser().getFollowersCount() > randomNumbers.get(0)) {
					isMatching = true;
				}
			}
			else {
				log.error("No random numbers in memory!");
			}
			
			
			// If this tweet passes matching criteria, add it to the match list
			if(isMatching) {
				matchingTweets.add(thisTweet);
				log.info("Tweet matches! Adding to match list, size=" + matchingTweets.size());// + ", country=" + thisTweet.getPlace().getCountryCode().toString());				
				if(thisTweet.getPlace() != null) {
					if(thisTweet.getPlace().getCountryCode() != null) {
						log.info("Tweet location: " + thisTweet.getPlace().getCountryCode().toString());
						
					}
				}
			}
			else {
				//log.info("Tweet does not match");
			}
		}

		
		return matchingTweets;
	}
	
	public void outputTweetsToFile(ArrayList<Status> tweets, String filePath) throws IOException {
		BufferedWriter ResultsFileWriter = new BufferedWriter(new FileWriter(filePath, true));
		for(Status thisTweet : tweets) {
			String tweetText = thisTweet.getText().replaceAll("(\\r|\\n)", "");
			ResultsFileWriter.append(tweetText + "\n");
		}
		ResultsFileWriter.close();
	}
	
	public void outputTopWordsToFile(ArrayList<Status> tweets, String filePath) throws IOException {
		
		HashMap<String, Integer> wordCounts = new HashMap<String, Integer>();
		String[] stopWords = new String[]{ "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves" };
		String thisTweetText;
		
		// First, compile a huge dictionary of word counts
		for(Status thisTweet : tweets) {
			// For retweets, use the original tweet text.
			if(thisTweet.isRetweet()) {
				thisTweetText = thisTweet.getRetweetedStatus().getText();
			}
			else {
				thisTweetText = thisTweet.getText();
			}
			
			// Strip out spaces, newlines, punctuation, and stop words
			thisTweetText = thisTweet.getText().replaceAll("(\\r|\\n|)", "").replace(',', ' ');
			log.info("thisTweetText=" + thisTweetText);
			String[] tweetWords = thisTweetText.split(" ");
			for(String thisWord : tweetWords) {
				if(!wordCounts.containsKey(thisWord)) {
					wordCounts.put(thisWord, 1);
				}
				else {
					wordCounts.put(thisWord, wordCounts.get(thisWord) + 1);
				}
			}
		}
		
		// Now eliminate the stop words
		for(String thisStopWord : stopWords) {
			if(wordCounts.containsKey(thisStopWord) || wordCounts.containsKey(WordUtils.capitalize(thisStopWord))) {
				wordCounts.remove(thisStopWord);
			}
		}
		wordCounts.remove("");
		
		// Now sort the list by word count
		Map<String, Integer> sortedWordCounts = sortByComparator(wordCounts, false);
		
		// Count the number of non-stop words.
		int nonStopWordCount = 0;
		Iterator it = sortedWordCounts.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			nonStopWordCount += (Integer)pair.getValue();
		}
		
		// Output two files that show the top 50% of words in the list:
		// 	1) A JSON structure matching words to frequency counts
		//	2) A CSV structure that just shows the raw words
		BufferedWriter ResultsFileWriter = new BufferedWriter(new FileWriter(filePath, true));
		BufferedWriter TopWordsCSVFileWriter = new BufferedWriter(new FileWriter("q2-output/all-intervals-topwords.csv", true));
		
		int numOutputWords = nonStopWordCount / 2;
		it = sortedWordCounts.entrySet().iterator();
		
		ResultsFileWriter.append("{ TopWordsArray: [\n");
		while (it.hasNext() && numOutputWords > 0) {
	        Map.Entry pair = (Map.Entry)it.next();
	        ResultsFileWriter.append("\t{ word: \"" + pair.getKey() + "\", count: " + pair.getValue() + " },\n");
	        TopWordsCSVFileWriter.append(pair.getKey() + ",");
	        //System.out.println(pair.getKey() + " = " + pair.getValue());
	        it.remove(); // avoids a ConcurrentModificationException
	        numOutputWords -= (Integer)pair.getValue();
	    }
		ResultsFileWriter.append("] }");
		TopWordsCSVFileWriter.append("\n");
		
		ResultsFileWriter.close();
		TopWordsCSVFileWriter.close();
	}
	
	
	
	/*
	 * Sorts a Map structure.
	 * Stolen from: http://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
	 */
	private static Map<String, Integer> sortByComparator(Map<String, Integer> unsortMap, final boolean order)
    {

        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
