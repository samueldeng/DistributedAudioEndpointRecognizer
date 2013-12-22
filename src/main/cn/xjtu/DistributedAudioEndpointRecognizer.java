package cn.xjtu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by samuel on 12/19/13.
 */
public class DistributedAudioEndpointRecognizer {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("WaveSegmentSpout", new WaveSegmentSpout(), 1);
        builder.setBolt("BasicSegmentProcessBolt",
                new BasicSegmentProcessBolt(), 5).shuffleGrouping(
                "WaveSegmentSpout");
        Config conf = new Config();
        conf.put("wavFile", "http://192.168.56.1/11k16bitpcm_5min.wav");
        conf.setDebug(true);
        try {
            //Cluster mode: use the args[0] to be the topology name.
            if (args != null && args.length > 0) {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } else {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("EndpointRecoginizer", conf, builder.createTopology());
                Utils.sleep(1000);
                cluster.killTopology("EndpointRecoginizer");
                cluster.shutdown();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
