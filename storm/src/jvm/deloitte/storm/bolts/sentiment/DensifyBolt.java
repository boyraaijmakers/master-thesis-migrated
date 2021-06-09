package deloitte.storm.bolts.sentiment;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class DensifyBolt extends BaseRichBolt {
    OutputCollector _collector;
    private HashMap<String, Double> _densifyLexicon;

    private final int NGRAM_MIN = 2;
    private final int NGRAM_MAX = 3;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
        initSentiStrength();
    }

    @Override
    public void execute(Tuple tuple) {
        // get the column word from tuple
        String corpus = tuple.getString(0);
        String[] words = corpus.split(" ");

        double totalScore = 0;
        for (int i = NGRAM_MIN; i <= NGRAM_MAX; i++){
            for (int j = 0; j < words.length - i; j++ ) {
                String word = words[j];
                for (int k = 1; k < i; k++ ) {
                    word += " " + words[j+k];
                }
                if (_densifyLexicon.containsKey(word)) totalScore += _densifyLexicon.get(word);
            }
        }

        int classifiedLabel = (totalScore >= 0.0D) ? 1 : -1;

        _collector.emit(tuple, new Values(Integer.toString(classifiedLabel), tuple.getString(1), tuple.getString(3)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classified-label", "true-label", "date"));
    }

    private void initSentiStrength() {
        _densifyLexicon = new HashMap<String, Double>();
        try {
            BufferedReader csv = new BufferedReader(new FileReader("C:\\Users\\braaijmakers\\Desktop\\Thesis\\Sentiment Analysis\\lexicons\\en_news_sentiment.txt"));
            String line;
            while ((line = csv.readLine()) != null) {
                String[] data = line.split(" ");
                double score = Double.parseDouble(data[1]);
                _densifyLexicon.put(data[0].replaceAll("_", " "), score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}