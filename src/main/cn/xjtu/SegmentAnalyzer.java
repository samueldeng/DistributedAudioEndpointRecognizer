package cn.xjtu;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;

/**
 * Created by samuel on 12/22/13.
 */
public class SegmentAnalyzer extends BaseBasicBolt {

    private static final Float MAXVALUE16bitSIGNED = (float) 32768;
    private static final Float MAXVALUE8bitSIGNED = (float) 128;
    private static final Float MAXVALUE8bitUNSIGNED = (float) 256;
    private static final double AMPTI_THREASHOLD = 0.05;
    private static final int LENGTH_THREASHOLD = 2500;

    private Object id;
    private Long segmentIndex;
    private Integer sampleSizeInBits;
    private Boolean isBigEndian;
    private Float frameRate;
    private Boolean isPCM_SIGNED;
    private byte[] audioBytes;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        // Parse the tuple from the SegmentSplitWithFormatParser.
        id = tuple.getValue(0);
        segmentIndex = tuple.getLongByField("segmentIndex");
        sampleSizeInBits = tuple.getIntegerByField("sampleSizeInBits");
        isBigEndian = tuple.getBooleanByField("isBigEndian");
        frameRate = tuple.getFloatByField("frameRate");
        isPCM_SIGNED = tuple.getBooleanByField("isPCM_SIGNED");
        audioBytes = tuple.getBinaryByField("audioBytes");


        ArrayList<Integer> frameDataList = new ArrayList<Integer>();
        ArrayList<Float> normalizedValues = new ArrayList<Float>();
        ArrayList<String> silencePoints = new ArrayList<String>();

        readAudioByte(audioBytes, frameDataList);
        normalizer(frameDataList, normalizedValues);
        basicSilenceFinder(normalizedValues, silencePoints);
        for (String point : silencePoints)
            collector.emit(new Values(id, point));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "silencePoint"));
    }

    private void readAudioByte(byte[] audioBytes, ArrayList<Integer> frameDataList) {
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
     * @param frameDataList The source frameDataList with raw value
     * @param norDataList   The destination.
     */
    private void normalizer(ArrayList<Integer> frameDataList, ArrayList<Float> norDataList) {
        if (sampleSizeInBits == 16) {
            // sampleSizeInBits is 16, default is signed.
            for (Integer aFrameDataList : frameDataList) {
                norDataList.add((float) aFrameDataList / MAXVALUE16bitSIGNED);
            }
        } else if (sampleSizeInBits == 8 && isPCM_SIGNED) {
            for (Integer frameData : frameDataList) {
                norDataList.add((float) frameData / MAXVALUE8bitSIGNED);
            }
        } else {
            for (Integer frameData : frameDataList) {
                norDataList.add((float) frameData / MAXVALUE8bitUNSIGNED);
            }
        }
    }

    /**
     * convert <tt>frameIndex</tt> into timePoint.
     *
     * @param frameIndex count in Frame.
     * @return timePoint count in seconds.
     */
    private String conToTimePoint(long frameIndex) {
        long frameIndexinWav = frameIndex + segmentIndex;
        float timePoint = (float) frameIndexinWav / frameRate;
        return Float.toString(timePoint);
    }

    /**
     * @param norValues the normalized value data list.
     * @param silencePoint the output silence point list.
     */
    private void basicSilenceFinder(ArrayList<Float> norValues, ArrayList<String> silencePoint) {
        long advancePtr, delayPtr;
        for (advancePtr = 0, delayPtr = 0; (delayPtr < norValues.size())
                && (advancePtr < norValues.size()); advancePtr++, delayPtr++) {
            // TODO The transform from long to integer will cause some trouble.
            if (Math.abs(norValues.get((int) advancePtr)) < AMPTI_THREASHOLD) {
                for (; advancePtr < norValues.size(); advancePtr++) {
                    if (Math.abs(norValues.get((int) advancePtr)) > AMPTI_THREASHOLD) {
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
