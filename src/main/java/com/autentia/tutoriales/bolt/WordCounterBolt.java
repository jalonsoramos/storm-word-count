package com.autentia.tutoriales.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCounterBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	private Map<String, MutableInt> words = null;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.words = new HashMap<String, MutableInt>();
	}

	@Override
	public void execute(Tuple input) {
		final String word = input.getStringByField("word");
		MutableInt count = words.get(word);
		if (count == null) {
			count = new MutableInt();
		}
		count.increment();

		words.put(word, count);
		collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}
