package deloitte.storm.bolts.preprocessing;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class NLTKPyBolt extends ShellBolt implements IRichBolt {

    public NLTKPyBolt() {
        super("python", "nltkBolt.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "sentiment-label", "company-label", "date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}