package deloitte.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import deloitte.storm.bolts.nel.DBPediaSpotlightBolt;
import deloitte.storm.bolts.preprocessing.*;
import deloitte.storm.bolts.reporting.*;
import deloitte.storm.bolts.ner.*;
import deloitte.storm.bolts.sentiment.SWNBolt;
import deloitte.storm.spouts.*;

class MainTopology {


    public static void main(String[] args) throws Exception
    {
        // create the topology
        TopologyBuilder builder = new TopologyBuilder();

        // attach the word spout to the topology - parallelism of 10
        builder.setSpout("input", new ArticleSpout("merged.csv", ";".charAt(0), false), 1);

        builder.setBolt("preprocess", new NLTKPyBolt(), 10).shuffleGrouping("input");

        builder.setBolt("sentiment", new SWNBolt(), 1).shuffleGrouping("preprocess");
        builder.setBolt("print-sentiment", new SentimentPrinterBolt(), 1).shuffleGrouping("sentiment");

        builder.setBolt("ner", new OpenNLPCompanyBolt(), 10).shuffleGrouping("preprocess");
        builder.setBolt("print-ner", new NERPrinterBolt(), 1).shuffleGrouping("ner");

        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("thesis-validation", conf, builder.createTopology());

            // let the topology run for 30 seconds. note topologies never terminate!
            Thread.sleep(1800000);

            // kill the topology
            cluster.killTopology("thesis-validation");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }
    }
}
