package storm.starter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		String[] stopWords = new String[]{ "a","able","about","above","abst","accordance","according","accordingly","across","act","actually","added","adj","affected","affecting","affects","after","afterwards","again","against","ah","all","almost","alone","along","already","also","although","always","am","among","amongst","an","and","announce","another","any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere","apparently","approximately","are","aren","arent","arise","around","as","aside","ask","asking","at","auth","available","away","awfully","b","back","be","became","because","become","becomes","becoming","been","before","beforehand","begin","beginning","beginnings","begins","behind","being","believe","below","beside","besides","between","beyond","biol","both","brief","briefly","but","by","c","ca","came","can","cannot","can't","cause","causes","certain","certainly","co","com","come","comes","contain","containing","contains","could","couldnt","d","date","did","didn't","different","do","does","doesn't","doing","done","don't","down","downwards","due","during","e","each","ed","edu","effect","eg","eight","eighty","either","else","elsewhere","end","ending","enough","especially","et","et-al","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","except","f","far","few","ff","fifth","first","five","fix","followed","following","follows","for","former","formerly","forth","found","four","from","further","furthermore","g","gave","get","gets","getting","give","given","gives","giving","go","goes","gone","got","gotten","h","had","happens","hardly","has","hasn't","have","haven't","having","he","hed","hence","her","here","hereafter","hereby","herein","heres","hereupon","hers","herself","hes","hi","hid","him","himself","his","hither","home","how","howbeit","however","hundred","i","id","ie","if","i'll","im","immediate","immediately","importance","important","in","inc","indeed","index","information","instead","into","invention","inward","is","isn't","it","itd","it'll","its","itself","i've","j","just","k","keep	keeps","kept","kg","km","know","known","knows","l","largely","last","lately","later","latter","latterly","least","less","lest","let","lets","like","liked","likely","line","little","'ll","look","looking","looks","ltd","m","made","mainly","make","makes","many","may","maybe","me","mean","means","meantime","meanwhile","merely","mg","might","million","miss","ml","more","moreover","most","mostly","mr","mrs","much","mug","must","my","myself","n","na","name","namely","nay","nd","near","nearly","necessarily","necessary","need","needs","neither","never","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","nor","normally","nos","not","noted","nothing","now","nowhere","o","obtain","obtained","obviously","of","off","often","oh","ok","okay","old","omitted","on","once","one","ones","only","onto","or","ord","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","owing","own","p","page","pages","part","particular","particularly","past","per","perhaps","placed","please","plus","poorly","possible","possibly","potentially","pp","predominantly","present","previously","primarily","probably","promptly","proud","provides","put","q","que","quickly","quite","qv","r","ran","rather","rd","re","readily","really","recent","recently","ref","refs","regarding","regardless","regards","related","relatively","research","respectively","resulted","resulting","results","right","run","s","said","same","saw","say","saying","says","sec","section","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sent","seven","several","shall","she","shed","she'll","shes","should","shouldn't","show","showed","shown","showns","shows","significant","significantly","similar","similarly","since","six","slightly","so","some","somebody","somehow","someone","somethan","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify","specifying","still","stop","strongly","sub","substantially","successfully","such","sufficiently","suggest","sup","sure","t","take","taken","taking","tell","tends","th","than","thank","thanks","thanx","that","that'll","thats","that've","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","thered","therefore","therein","there'll","thereof","therere","theres","thereto","thereupon","there've","these","they","theyd","they'll","theyre","they've","think","this","those","thou","though","thoughh","thousand","throug","through","throughout","thru","thus","til","tip","to","together","too","took","toward","towards","tried","tries","truly","try","trying","ts","twice","two","u","un","under","unfortunately","unless","unlike","unlikely","until","unto","up","upon","ups","us","use","used","useful","usefully","usefulness","uses","using","usually","v","value","various","'ve","very","via","viz","vol","vols","vs","w","want","wants","was","wasnt","way","we","wed","welcome","we'll","went","were","werent","we've","what","whatever","what'll","whats","when","whence","whenever","where","whereafter","whereas","whereby","wherein","wheres","whereupon","wherever","whether","which","while","whim","whither","who","whod","whoever","whole","who'll","whom","whomever","whos","whose","why","widely","willing","wish","with","within","without","wont","words","world","would","wouldnt","www","x","y","yes","yet","you","youd","you'll","your","youre","yours","yourself","yourselves","you've","z","zero","rt" };
		String thisTweetText;
		
		// First, compile a huge dictionary of word counts
		for(Status thisTweet : tweets) {
			
			// For retweets, use the original tweet text.
			if(thisTweet.isRetweet()) {
				thisTweetText = thisTweet.getRetweetedStatus().getText().toLowerCase();
			}
			else {
				thisTweetText = thisTweet.getText().toLowerCase();
			}
			
			// Strip out newlines
			thisTweetText = thisTweetText.replaceAll("(\\r|\\n|)", "");
			
			String[] tweetWords = thisTweetText.split(" ");
			for(String thisWord : tweetWords) {
				
				// Clean up the word using a regular expression
				Pattern wordCleanPattern = Pattern.compile("^[\"',.?!;:()]*([a-z]([a-z'\\-]*[a-z])?)[\"',.?!;:()]*$");
				Matcher wordCleanMatcher = wordCleanPattern.matcher(thisWord);
				String cleanedWord = thisWord;
				
				// If the word checks out, add it to the word countsl ist
				if(wordCleanMatcher.matches()) {
					cleanedWord = wordCleanMatcher.group(1).toString();
					if(!wordCounts.containsKey(cleanedWord)) {
						wordCounts.put(cleanedWord, 1);
					}
					else {
						wordCounts.put(cleanedWord, wordCounts.get(cleanedWord) + 1);
					}
			    }				
			}
		}
		
		// Now eliminate the stop words
		for(String thisStopWord : stopWords) {
			if(wordCounts.containsKey(thisStopWord)) {
				wordCounts.remove(thisStopWord);
			}
		}
		
		// Now sort the list by word count
		Map<String, Integer> sortedWordCounts = sortByComparator(wordCounts, false);
		
		// Output two files that show the top 50% of words in the list:
		// 	1) A JSON structure matching words to frequency counts
		//	2) A CSV structure that just shows the raw words
		BufferedWriter ResultsFileWriter = new BufferedWriter(new FileWriter(filePath, true));
		BufferedWriter TopWordsCSVFileWriter = new BufferedWriter(new FileWriter("q2-output/all-intervals-topwords.csv", true));
		
		int numOutputWords = sortedWordCounts.size() / 2;
		Iterator it = sortedWordCounts.entrySet().iterator();
		
		ResultsFileWriter.append("{ TopWordsArray: [\n");
		while (it.hasNext() && numOutputWords > 0) {
	        Map.Entry pair = (Map.Entry)it.next();
	        ResultsFileWriter.append("\t{ word: \"" + pair.getKey() + "\", count: " + pair.getValue() + " },\n");
	        TopWordsCSVFileWriter.append(pair.getKey() + ",");
	        it.remove(); // avoids a ConcurrentModificationException
	        numOutputWords --;
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
