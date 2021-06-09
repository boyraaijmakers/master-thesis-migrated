package deloitte.storm.bolts.ner;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class OpenNLPCompanyBolt extends BaseRichBolt {
    OutputCollector _collector;
    private static NameFinderME nameFinder;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
        initOpenNLP();
    }

    @Override
    public void execute(Tuple tuple) {
        // get the column word from tuple
        String corpus = tuple.getString(0);
        String trueCompany = tuple.getString(1);

        try {
            String[] sentence = corpus.split(" ");
            Span nameSpans[] = nameFinder.find(sentence);

            HashMap<String, Integer> res = new HashMap<>();

            for(Span span : nameSpans) {
                String word = sentence[span.getStart()];
                if(word == "") continue;
                if (res.containsKey(word)) {
                    res.put(word, res.get(word) + 1);
                } else {
                    res.put(word, 1);
                }
            }

            String extractedCompany = "";
            int max = 0;

            for (String comp : res.keySet()){
                if (res.get(comp) > max) {
                    extractedCompany = comp;
                    max = res.get(comp);
                }
            }

            _collector.emit(tuple, new Values(extractedCompany, trueCompany));

        } catch (Exception e) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classified-label", "true-label"));
    }

    private void initOpenNLP() {
        try {
            InputStream inputStreamNameFinder = new FileInputStream("C:\\Users\\braaijmakers\\Desktop\\Thesis\\NER\\apache-opennlp-1.9.1\\en-ner-organization.bin");
            TokenNameFinderModel model = new TokenNameFinderModel(inputStreamNameFinder);
            nameFinder = new NameFinderME(model);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}