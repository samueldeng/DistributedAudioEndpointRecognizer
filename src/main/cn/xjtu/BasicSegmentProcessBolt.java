package cn.xjtu;

import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by samuel on 12/19/13.
 */
public class BasicSegmentProcessBolt extends BaseRichBolt {

    private static final Float MAXVALUE16bitSIGNED = (float) 32768;
    private static final Float MAXVALUE8bitSIGNED = (float) 128;
    private static final Float MAXVALUE8bitUNSIGNED = (float) 256;
    private static final double AMPTI_THREASHOLD = 0.05;
    private static final int LENGTH_THREASHOLD = 2500;

    long segmentIndex;
    int sampleSizeInBits;
    boolean isBigEndian;
    float frameRate;
    boolean isPCM_SIGNED;

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        byte[] audioBytes;
        ArrayList<Integer> frameDataList = new ArrayList<Integer>();
        ArrayList<Float> normalizedValue = new ArrayList<Float>();
        ArrayList<String> silencePoint = new ArrayList<String>();

        // Parse the tuple from the WaveSegmentSpout.
        segmentIndex = input.getLongByField("segmentIndex");
        sampleSizeInBits = input.getIntegerByField("sampleSizeInBits");
        isBigEndian = input.getBooleanByField("isBigEndian");
        frameRate = input.getFloatByField("frameRate");
        isPCM_SIGNED = input.getBooleanByField("isPCM_SIGNED");
        audioBytes = input.getBinaryByField("audioBytes");

        readAudioByte(audioBytes, frameDataList);
        normalizer(frameDataList, normalizedValue);
        basicSilenceFinder(normalizedValue, silencePoint);
        for (String aSilencePoint : silencePoint)
            collector.emit(new Values(aSilencePoint));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("silencePoint"));
    }

    /**
     * read <tt>audioBytes</tt> into frame according to the sapmleSizeinFram,
     * Big Endian and isPCM_SIGNED
     *
     * @param audioBytes
     * the raw byte filled with the audio data.
     * @param frameDataList
     * the frameDataList filled the quantized value.
     */
    private void readAudioByte(byte[] audioBytes,
                               ArrayList<Integer> frameDataList) {
        if (sampleSizeInBits == 16) {
            if (isBigEndian) {
                for (int i = 0; i < audioBytes.length / 2; i++) {
                                        /* First byte is MSB (high order) */
                    int MSB = audioBytes[2 * i];
                                        /* Second byte is LSB (low order) */
                    int LSB = audioBytes[2 * i + 1];
                    frameDataList.add(MSB << 8 | (255 & LSB));
                }
            } else {
                for (int i = 0; i < audioBytes.length / 2; i++) {
                                        /* First byte is LSB (low order) */
                    int LSB = audioBytes[2 * i];
                                        /* Second byte is MSB (high order) */
                    int MSB = audioBytes[2 * i + 1];
                    frameDataList.add(MSB << 8 | (255 & LSB));
                }
            }
        } else {
            if (sampleSizeInBits == 8) {
                if (isPCM_SIGNED) {
                    // PCM_SIGNED
                    for (byte audioByte : audioBytes) {
                        frameDataList.add((int) audioByte);
                    }
                } else {
                    // PCM_UNSIGNED
                    for (byte audioByte : audioBytes) {
                        frameDataList.add((int) audioByte - 128);
                    }
                }
            }
        }

    }

    /**
     * normalize <tt>frameDataList</tt> and put all normalized value to
     * <tt>norDataList</tt>
     *
     * @param frameDataList
     * The source frameDataList with raw value
     * @param norDataList
     * The destination.
     */
    private void normalizer(ArrayList<Integer> frameDataList,
                            ArrayList<Float> norDataList) {
        if (sampleSizeInBits == 16) {
            // sampleSizeInBits is 16, default is signed.
            for (int i = 0; i < frameDataList.size(); i++) {
                norDataList.add(new Float(frameDataList.get(i))
                        / MAXVALUE16bitSIGNED);
            }
        } else if (sampleSizeInBits == 8 && isPCM_SIGNED) {
            for (int i = 0; i < frameDataList.size(); i++) {
                norDataList.add(new Float(frameDataList.get(i))
                        / MAXVALUE8bitSIGNED);
            }
        } else {
            for (int i = 0; i < frameDataList.size(); i++) {
                norDataList.add(new Float(frameDataList.get(i))
                        / MAXVALUE8bitUNSIGNED);
            }
        }
    }

    /**
     * convert <tt>frameIndex</tt> into timePoint.
     *
     * @param frameIndex
     * count in Frame.
     * @return timePoint count in seconds.
     */
    private String conToTimePoint(long frameIndex) {
        long frameIndexinWav = frameIndex + segmentIndex;
        float timePoint = (float) frameIndexinWav / frameRate;
        return Float.toString(timePoint);
    }

    /**
     * @param norValue
     * @param silencePoint
     */
    private void basicSilenceFinder(ArrayList<Float> norValue,
                                    ArrayList<String> silencePoint) {
        long advancePtr = 0, delayPtr = 0;
        for (advancePtr = 0, delayPtr = 0; (delayPtr < norValue.size())
                && (advancePtr < norValue.size()); advancePtr++, delayPtr++) {
            // TODO The transform from long to integer will cause some trouble.
            if (Math.abs(norValue.get((int) advancePtr)) < AMPTI_THREASHOLD) {
                for (; advancePtr < norValue.size(); advancePtr++) {
                    if (Math.abs(norValue.get((int) advancePtr)) > AMPTI_THREASHOLD) {
                        if (advancePtr - delayPtr < LENGTH_THREASHOLD) {
                            break;
                        }
                        if ((advancePtr - delayPtr) > LENGTH_THREASHOLD) {
                            silencePoint.add(conToTimePoint(advancePtr));
                            break;
                        }
                    }
                }
                delayPtr = advancePtr;
            }
        }
    }
}
