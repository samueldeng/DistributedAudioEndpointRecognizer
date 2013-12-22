package cn.xjtu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * Created by samuel on 12/19/13.
 */
public class DistributedAudioEndpointRecognizer {
    public static void main(String[] args) {

        // create the DRPC topology.
        // TODO this method seems to be deprecated, using the trident api to upgrade it.
        // FIXME filedsgroup.............
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("EPRecog");
        builder.addBolt(new SegmentSplitWithFormatParser(), 5);
        builder.addBolt(new SegmentAnalyzer(), 19);
        builder.addBolt(new ReportSummary(), 5);

        // create the configuration.
        Config conf = new Config();
        conf.setDebug(true);

        // starting running on local or cluster.
        try {
            if (args == null || args.length == 0) {
                LocalDRPC drpc = new LocalDRPC();
                LocalCluster cluster = new LocalCluster();

                cluster.submitTopology("EPRecog-drpc", conf, builder.createLocalTopology(drpc));
                String[] urlToTry = new String[]{ "http://192.168.56.1/11k16bitpcm_5min.wav", "http://192.168.56.1/11k16bitpcm_5min.wav"};
                for (String url : urlToTry){
                    System.out.println(drpc.execute("EPRecog", url));
                }
                cluster.shutdown();
                drpc.shutdown();
            } else {
                conf.setNumWorkers(6);
                StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
