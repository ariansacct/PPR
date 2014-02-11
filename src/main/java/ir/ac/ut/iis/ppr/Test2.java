package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.GraphJob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 1/9/14
 * Time: 10:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class Test2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob doublingPPR2Job = DoublingPPR2.createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (doublingPPR2Job.waitForCompletion(true))
            System.out.println("doublingPPR2Job Finished in "
                    + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
}
