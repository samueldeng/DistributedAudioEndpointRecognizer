package cn.xjtu;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import java.net.URL;

/**
 * Created by samuel on 12/22/13.
 */
public class SegmentSplitWithFormatParser extends BaseBasicBolt {

    private static final int SEGMENTSIZETHREASHOLD = 1024 * 1024;

    private Object id;
    private String wavURL;


    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        id = tuple.getValue(0);
        wavURL = tuple.getString(1);

        try {
            URL _wavURL = new URL(wavURL);
            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(_wavURL);
            // DEBUG_INFO
            System.out.println("DEBUG_INFO   " + Thread.currentThread().getName());
            // Get the format parameters from the audioInputStream.
            AudioFormat format = audioInputStream.getFormat();
            long frameLength = audioInputStream.getFrameLength();
            int frameSize = format.getFrameSize();
            int sampleSizeInBits = format.getSampleSizeInBits();
            boolean isBigEndian = format.isBigEndian();
            float frameRate = format.getFrameRate();
            boolean isPCM_SIGNED = format.getEncoding().toString().startsWith("PCM_SIGN");

            // Split the audio segment to the next bolt.
            long segmentIndex = 0;
            byte[] tempAudioBytes = new byte[SEGMENTSIZETHREASHOLD * frameSize];
            int sizeRead;
            while ((sizeRead = audioInputStream.read(tempAudioBytes)) != -1) {
                byte[] nextAudioBytes = new byte[sizeRead];
                System.arraycopy(tempAudioBytes, 0, nextAudioBytes, 0, sizeRead);
                collector.emit(
                        new Values(
                                id,
                                nextAudioBytes,
                                segmentIndex,
                                sampleSizeInBits,
                                isBigEndian,
                                frameRate,
                                isPCM_SIGNED
                        )
                );
                segmentIndex += SEGMENTSIZETHREASHOLD;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(
                new Fields(
                        "id",
                        "audioBytes",
                        "segmentIndex",
                        "sampleSizeInBits",
                        "isBigEndian",
                        "frameRate",
                        "isPCM_SIGNED"
                )
        );
    }

}
