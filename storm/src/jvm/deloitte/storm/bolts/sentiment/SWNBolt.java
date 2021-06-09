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
import java.util.*;

public class SWNBolt extends BaseRichBolt
{
    OutputCollector _collector;
    private HashMap<String, Double> _swn3Dict;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector)
    {
        _collector = collector;
        initSWN();
    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the column word from tuple
        String corpus = tuple.getString(0);

        String[] words = corpus.split(" ");
        double totalScore = 0;
        for (String word : words) {

            Double total = 0D;
            if (_swn3Dict.get(word + "#n") != null) total += _swn3Dict.get(word + "#n");
            if (_swn3Dict.get(word + "#a") != null) total += _swn3Dict.get(word + "#a");
            if (_swn3Dict.get(word + "#r") != null) total += _swn3Dict.get(word + "#r");
            if (_swn3Dict.get(word + "#v") != null) total += _swn3Dict.get(word + "#v");

            totalScore += total;
        }

        int classifiedLabel = (totalScore >= 0.0) ? 1 : -1;

        _collector.emit(tuple, new Values(Integer.toString(classifiedLabel), tuple.getString(1), tuple.getString(3)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("classified-label", "true-label", "date"));
    }

    private void initSWN(){
        _swn3Dict = new HashMap<String, Double>();
        HashMap<String, Vector<Double>> _temp = new HashMap<>();
        try {
            BufferedReader csv = new BufferedReader(new FileReader("SentiWordNet3.txt"));
            String line = "";
            while ((line = csv.readLine()) != null) {
                if (line.startsWith("#")) continue;
                String[] data = line.split("\t");
                Double score = Double.parseDouble(data[2]) - Double.parseDouble(data[3]);
                String[] words = data[4].split(" ");
                for (String w : words) {
                    String[] w_n = w.split("#");
                    w_n[0] += "#" + data[0];
                    int index = Integer.parseInt(w_n[1]) - 1;
                    if (_temp.containsKey(w_n[0])) {
                        Vector<Double> v = _temp.get(w_n[0]);
                        if (index > v.size())
                            for (int i = v.size(); i < index; i++)
                                v.add(0.0);
                        v.add(index, score);
                        _temp.put(w_n[0], v);
                    } else {
                        Vector<Double> v = new Vector<Double>();
                        for (int i = 0; i < index; i++)
                            v.add(0.0);
                        v.add(index, score);
                        _temp.put(w_n[0], v);
                    }
                }
            }
            Set<String> temp = _temp.keySet();
            for (Iterator<String> iterator = temp.iterator(); iterator.hasNext(); ) {
                String word = iterator.next();
                Vector<Double> v = _temp.get(word);
                double score = 0.0;
                double sum = 0.0;
                for (int i = 0; i < v.size(); i++)
                    score += (1D / (double) (i + 1)) * v.get(i);
                for (int i = 1; i <= v.size(); i++)
                    sum += 1D / (double) i;
                score /= sum;
                _swn3Dict.put(word, score);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}