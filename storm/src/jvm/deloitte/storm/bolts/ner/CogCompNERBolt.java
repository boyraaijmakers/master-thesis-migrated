package deloitte.storm.bolts.ner;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import edu.illinois.cs.cogcomp.core.datastructures.textannotation.TextAnnotation;
import edu.illinois.cs.cogcomp.nlp.utility.TokenizerTextAnnotationBuilder;
import edu.illinois.cs.cogcomp.annotation.AnnotatorException;
import edu.illinois.cs.cogcomp.annotation.TextAnnotationBuilder;
import edu.illinois.cs.cogcomp.core.datastructures.ViewNames;
import edu.illinois.cs.cogcomp.ner.NERAnnotator;
import edu.illinois.cs.cogcomp.nlp.tokenizer.StatefulTokenizer;
import java.io.IOException;

import java.util.Map;

public class CogCompNERBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        // Create a TextAnnotation using the LBJ sentence splitter
        // and tokenizers.
        TextAnnotationBuilder tab = new TokenizerTextAnnotationBuilder(new StatefulTokenizer( false));

        TextAnnotation ta = tab.createTextAnnotation(tuple.getString(0));
        try {
            NERAnnotator co = new NERAnnotator(ViewNames.NER_CONLL);
            co.getView(ta);
        } catch (Exception e ){
            e.printStackTrace();
        }

        _collector.emit(tuple, new Values(ta.getView(ViewNames.NER_CONLL), tuple.getString(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("classified-label", "true-label"));
    }
}
