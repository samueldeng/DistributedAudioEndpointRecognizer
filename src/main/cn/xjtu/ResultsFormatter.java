package cn.xjtu;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by samuel on 12/23/13.
 */
public class ResultsFormatter extends BaseBatchBolt {
    BatchOutputCollector collector;
    Object id;
    List<String> endPoints = new ArrayList<String>();

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        this.collector = collector;
        this.id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        endPoints.add(tuple.getStringByField("silencePoint"));
    }

    @Override
    public void finishBatch() {
        String emit = null;
        for (String endPoint : endPoints) {
            emit += endPoint;
        }
        collector.emit(new Values(id, emit));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "endpoints"));
    }
}
