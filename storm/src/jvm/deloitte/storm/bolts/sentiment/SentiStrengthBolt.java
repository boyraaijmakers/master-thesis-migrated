package deloitte.storm.bolts.sentiment;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import uk.ac.wlv.sentistrength.*;

import java.util.Map;

public class SentiStrengthBolt extends BaseRichBolt {
    OutputCollector _collector;
    private SentiStrength sentiStrength;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
        initSentiStrength();
    }

    @Override
    public void execute(Tuple tuple) {
        // get the column word from tuple
        String corpus = tuple.getString(0);
        String trueLabel = tuple.getString(1);

        String[] sentences = corpus.split(".");

        int score = 0;

        for (int i = 0; i < sentences.length; i++) {
            String[] res = sentiStrength.computeSentimentScores(sentences[i]).split(" ");
            score += Integer.parseInt(res[2]);
        }

        int classifiedLabel = score >= 0 ? 1 : -1;

        _collector.emit(tuple, new Values(Integer.toString(classifiedLabel), trueLabel));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classified-label", "true-label"));
    }

    private void initSentiStrength() {
        sentiStrength = new SentiStrength();
        String ssthInitialisation[] = {"sentidata", "path", "scale"};
        sentiStrength.initialise(ssthInitialisation);
    }

}