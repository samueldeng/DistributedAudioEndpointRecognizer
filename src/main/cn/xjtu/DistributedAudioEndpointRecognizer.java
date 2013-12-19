package cn.xjtu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by samuel on 12/19/13.
 */
public class DistributedAudioEndpointRecognizer {
    public static void main(String[] args) {
        TopologyBuilder topoBuilder = new TopologyBuilder();

        topoBuilder.setSpout("WaveSegmentSpout", new WaveSegmentSpout(), 1);
        topoBuilder.setBolt("BasicSegmentProcessBolt",
                new BasicSegmentProcessBolt(), 5).shuffleGrouping(
                "WaveSegmentSpout");
        Config conf = new Config();
        conf.put(
                "wavFile",
                "/home/samuel/workspace/DistributedAudioEndpointRecognizer/11k16bitpcm_5min.wav");
        conf.setNumWorkers(5);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("EndpointRecoginizer", conf,
                topoBuilder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("EndpointRecoginizer");
        cluster.shutdown();
    }
}
