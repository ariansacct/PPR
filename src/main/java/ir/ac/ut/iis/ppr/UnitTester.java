package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.GraphJob;

import java.io.IOException;

/**
 * Created by arian on 2/14/14.
 */
public class UnitTester {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        SegmentGenerator.LAMBDA = 8;
        SegmentGenerator.THETA = 3;
        SegmentGenerator.NO_SEGMENTS = (int) Math.ceil((double) SegmentGenerator.LAMBDA / (double) SegmentGenerator.THETA);
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob segmentGeneratorJob = SegmentGenerator.createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (segmentGeneratorJob.waitForCompletion(true))
            System.out.println("Our segmentGeneratorJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds.");
    }
}
