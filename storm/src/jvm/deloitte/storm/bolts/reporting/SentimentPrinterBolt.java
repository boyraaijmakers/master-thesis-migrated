package deloitte.storm.bolts.reporting;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * A bolt that prints the word and count to redis
 */
public class SentimentPrinterBolt extends BaseRichBolt
{
    // place holder to keep the connection to redis
    transient RedisConnection<String,String> redis;
    int count;
    int correct;

    @Override
    public void prepare(
            Map                     map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost",6379);

        // initiate the actual connection
        redis = client.connect();

        count = 0;
        correct = 0;
    }

    @Override
    public void execute(Tuple tuple)
    {

        String classifiedLabel = tuple.getString(0);
        String trueLabel = tuple.getString(1);
        String date = tuple.getString(2);

        String result = classifiedLabel.equals("1") ? (trueLabel.equals("1") ? "TP" : "FP") : (trueLabel.equals("1") ? "FN" : "TN");
        // publish the word count to redis using word as the key
        redis.publish("SentimentReport", date + ";" + trueLabel + ";" + classifiedLabel + ";" + result);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}
