package cn.xjtu;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Created by samuel on 12/19/13.
 */
public class WaveSegmentSpout extends BaseRichSpout {
    private static final int SEGMENTSIZETHREASHOLD = 1024 * 1024;// count in
    // frame.

    public AudioInputStream audioInputStream;
    public long frameLength;
    public int frameSize;
    public int sampleSizeInBits;
    public boolean isBigEndian;
    public float frameRate;
    public boolean isPCM_SIGNED;

    public SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            File wavFile = new File(conf.get("wavFile").toString());
            FileInputStream fis = new FileInputStream(wavFile);
            byte[] arrFile = new byte[(int) wavFile.length()];
            fis.read(arrFile);
            ByteArrayInputStream bis = new ByteArrayInputStream(arrFile);
            audioInputStream = AudioSystem.getAudioInputStream(bis);

            AudioFormat format = audioInputStream.getFormat();
            frameLength = audioInputStream.getFrameLength();
            frameSize = format.getFrameSize();
            sampleSizeInBits = format.getSampleSizeInBits();
            isBigEndian = format.isBigEndian();
            frameRate = format.getFrameRate();
            isPCM_SIGNED = format.getEncoding().toString()
                    .startsWith("PCM_SIGN");
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void nextTuple() {
        boolean completed = false;
        if (completed)
            return;
        long segmentIndex = 0;
        byte[] tempAudioBytes = new byte[SEGMENTSIZETHREASHOLD * frameSize];
        try {
            int sizeRead;
            while ((sizeRead = audioInputStream.read(tempAudioBytes)) != -1) {
                byte[] nextAudioBytes = new byte[sizeRead];
                System.arraycopy(tempAudioBytes, 0, nextAudioBytes, 0, sizeRead);
                collector
                        .emit(new Values(nextAudioBytes, segmentIndex,
                                sampleSizeInBits, isBigEndian, frameRate,
                                isPCM_SIGNED));
                segmentIndex += SEGMENTSIZETHREASHOLD;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            completed = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("audioBytes", "segmentIndex",
                "sampleSizeInBits", "isBigEndian", "frameRate", "isPCM_SIGNED"));
    }


}