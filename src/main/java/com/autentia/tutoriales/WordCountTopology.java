package com.autentia.tutoriales;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.autentia.tutoriales.bolt.CountPrinterBolt;
import com.autentia.tutoriales.bolt.TweetSplitterBolt;
import com.autentia.tutoriales.bolt.WordCounterBolt;
import com.autentia.tutoriales.spout.TweetsStreamingConsumerSpout;

public class WordCountTopology {

	public static void main(String... args) throws AlreadyAliveException, InvalidTopologyException {
		final TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitterSpout", new TweetsStreamingConsumerSpout());
		builder.setBolt("tweetSplitterBolt", new TweetSplitterBolt(), 10).shuffleGrouping("twitterSpout");
		builder.setBolt("wordCounterBolt", new WordCounterBolt(), 10).fieldsGrouping("tweetSplitterBolt", new Fields("word"));
		builder.setBolt("countPrinterBolt", new CountPrinterBolt(), 10).fieldsGrouping("wordCounterBolt", new Fields("word"));

		final Config conf = new Config();
		conf.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordCountTopology", conf, builder.createTopology());
	}
}
