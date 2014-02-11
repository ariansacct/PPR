package ir.ac.ut.iis.ppr;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.CombineExample;
/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 11/25/13
 * Time: 1:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class MyGraphGenerator {
    protected static Log LOG = LogFactory.getLog(MyGraphGenerator.class);

    private static String SIZE_OF_MATRIX = "size.of.matrix";
    private static String DENSITY = "density.of.matrix";

    public static class MyGraphGeneratorBSP extends
            BSP<NullWritable, NullWritable, Text, TextArrayWritable, Text> {

        private Configuration conf;
        private int sizeN;
        private int density;
        private Map<Integer, HashSet<Integer>> list = new HashMap<Integer, HashSet<Integer>>();

        @Override
        public void setup(
                BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer) {
            this.conf = peer.getConfiguration();
            sizeN = conf.getInt(SIZE_OF_MATRIX, 10);
            density = conf.getInt(DENSITY, 1);
        }


        @Override
        public void bsp(
                BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer)
                throws IOException, SyncException, InterruptedException {
            int interval = sizeN / peer.getNumPeers();
            int startID = peer.getPeerIndex() * interval;
            int endID;
            if (peer.getPeerIndex() == peer.getNumPeers() - 1)
                endID = sizeN;
            else
                endID = startID + interval;

            // Generate N*(N+1) elements for lower triangular
            for (int i = startID; i < endID; i++) {
                HashSet<Integer> edges = new HashSet<Integer>();
                for (int j = 0; j <= i; j++) {
                    boolean nonZero = new Random().nextInt(density) == 0;
//                    if (nonZero && !edges.contains(j) && i != j) {
//                    if (true) {
                    if (j != i) {
                        edges.add(j);

                        // allocate remainders to the last task
                        int peerIndex = j / interval;
                        if (peerIndex == peer.getNumPeers())
                            peerIndex = peerIndex - 1;

                        peer.send(peer.getPeerName(peerIndex), new Text(j + "," + i));
                    }
                }

                list.put(i, edges);
            }

            // Synchronize the upper and lower
            peer.sync();
            Text received;
            while ((received = peer.getCurrentMessage()) != null) {
                String[] kv = received.toString().split(",");
                HashSet<Integer> nList = list.get(Integer.parseInt(kv[0]));
                nList.add(Integer.parseInt(kv[1]));
                list.put(Integer.parseInt(kv[0]), nList);
            }
        }

        @Override
        public void cleanup(
                BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer)
                throws IOException {
            for (Map.Entry<Integer, HashSet<Integer>> e : list.entrySet()) {
                Writable[] values = new Writable[e.getValue().size()];
                if (values.length > 0) {
                    int i = 0;
                    for (Integer v : e.getValue()) {
                        values[i] = new Text(String.valueOf(v));
                        i++;
                    }

                    TextArrayWritable value = new TextArrayWritable();
                    value.set(values);
                    peer.write(new Text(String.valueOf(e.getKey())), value);
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException,
            IOException, ClassNotFoundException {
        if (args.length < 4) {
            System.out
                    .println("Usage: <size n> <1/x density> <output path> <number of tasks>");
            System.exit(1);
        }

        // BSP job configuration
        HamaConfiguration conf = new HamaConfiguration();

        conf.setInt(SIZE_OF_MATRIX, Integer.parseInt(args[0]));
        conf.setInt(DENSITY, Integer.parseInt(args[1]));

        BSPJob bsp = new BSPJob(conf, CombineExample.class);
        // Set the job name
        bsp.setJobName("My Matrix Generator");
        bsp.setBspClass(MyGraphGeneratorBSP.class);
        bsp.setInputFormat(NullInputFormat.class);
        bsp.setOutputKeyClass(Text.class);
        bsp.setOutputValueClass(TextArrayWritable.class);
        bsp.setOutputFormat(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(bsp, new Path(args[2]));
        bsp.setNumBspTask(Integer.parseInt(args[3]));

        long startTime = System.currentTimeMillis();
        if (bsp.waitForCompletion(true)) {
            System.out.println("Job Finished in "
                    + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }
    }
}
