package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.GraphJob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 10/4/13
 * Time: 3:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class Test {
    public static void main(String[] args) throws IOException, FileNotFoundException,
            UnsupportedEncodingException, ClassNotFoundException, InterruptedException {
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob segmentGeneratorJob = SegmentGenerator.createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (segmentGeneratorJob.waitForCompletion(true)) {

            System.out.println("segmentGeneratorJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }

        GraphJob singleRandomWalkSQRTJob = SingleRandomWalkSQRT.createJob(args, conf);
        long startTime2 = System.currentTimeMillis();
        if (singleRandomWalkSQRTJob.waitForCompletion(true))
            System.out.println("singleRandomWalkSQRTJob Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    }
}
