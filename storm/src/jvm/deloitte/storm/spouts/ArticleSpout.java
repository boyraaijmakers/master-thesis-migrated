package deloitte.storm.spouts;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.opencsv.CSVReader;

import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class ArticleSpout extends BaseRichSpout {
    private String fileName;
    private char separator;
    private boolean includesHeaderRow;
    private SpoutOutputCollector _collector;
    private CSVReader reader;
    private AtomicLong linesRead;

    public ArticleSpout(String filename, char separator, boolean includesHeaderRow) {
        this.fileName = filename;
        this.separator = separator;
        this.includesHeaderRow = includesHeaderRow;
        linesRead=new AtomicLong(0);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            reader = new CSVReader(new FileReader(fileName), separator);
            // read and ignore the header if one exists
            if (includesHeaderRow) reader.readNext();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String[] line = reader.readNext();
            if (line != null) {
                long id=linesRead.incrementAndGet();
                _collector.emit(new Values(line[3], line[7], line[4], line[0]),id);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("body", "sentiment-label", "company-label", "date"));
    }

}
