package deloitte.storm.bolts.ner;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class SpacyNERPyBolt extends ShellBolt implements IRichBolt {

    public SpacyNERPyBolt() {
        super("python", "spacyNERBolt.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classified-label", "true-label", "date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
