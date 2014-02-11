package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 1/9/14
 * Time: 11:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class DoublingPPR2 {

    static PrintWriter writer;
    static PrintWriter finalWriter;
    static Map<Text, StringWritable> walks = new HashMap<Text, StringWritable>();
    static int L = 20;
    static double EPSILON = 0.15;
    static int R = (int) (L * EPSILON);

    public static class DoublingPPR2Vertex extends Vertex<Text, NullWritable, StringWritable> {

        @Override
        public void compute(Iterable<StringWritable> stringWritables) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
            if (this.getSuperstepCount() == 0) {
                String value = new String();
                this.setValue(new StringWritable(value));
            }

            else if (this.getSuperstepCount() > 0) {
                String temp = new String();
                temp = this.getValue().get();
                int lambda = Math.abs(StdRandom.geometric(EPSILON));
                SegmentGenerator.LAMBDA = lambda;
                SegmentGenerator.THETA = 1;
                SegmentGenerator.NO_SEGMENTS = (int) ((double) SegmentGenerator.LAMBDA
                        / (double) SegmentGenerator.THETA + 1);
                String[] args = new String[2];
                args[0] = "input";
                args[1] = "sgoutput";

                try {
                    SegmentGenerator.main(args);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                try {
                    SingleRandomWalkSQRT.main(args);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                String newTemp = new String();
                newTemp = temp + SingleRandomWalkSQRT.walks.get(this.getVertexID()).get();
                if (this.getSuperstepCount() != this.getMaxIteration())
                    newTemp = newTemp + ";";





            }
        }
    }

    public static class DoublingPPR2SeqReader extends VertexInputReader<Text, TextArrayWritable, Text, NullWritable, StringWritable> {
        @Override
        public boolean parseVertex(Text key, TextArrayWritable value, Vertex<Text, NullWritable, StringWritable> vertex) throws Exception {
            vertex.setVertexID(key);

            for (Writable v : value.get()) {
                vertex.addEdge(new Edge<Text, NullWritable>((Text) v, null));
            }

            return true;
        }
    }

    public static GraphJob createJob(String[] args, HamaConfiguration conf)
            throws IOException {

        GraphJob doublingPPR2Job = new GraphJob(conf, DoublingPPR2.class);
        doublingPPR2Job.setJobName("DoublingPPR2");
        doublingPPR2Job.setVertexClass(DoublingPPR2Vertex.class);
        doublingPPR2Job.setInputPath(new Path(args[0]));
        doublingPPR2Job.setOutputPath(new Path(args[1]));
//        doublingPPR2Job.setMaxIteration(DoublingPPR2.R);
        doublingPPR2Job.setMaxIteration(1);
        doublingPPR2Job.set("hama.graph.self.ref", "true");
        doublingPPR2Job.set("hama.graph.max.convergence.error", "0.001");
        doublingPPR2Job.setVertexInputReaderClass(DoublingPPR2SeqReader.class);
        doublingPPR2Job.setVertexIDClass(Text.class);
        doublingPPR2Job.setVertexValueClass(StringWritable.class);
        doublingPPR2Job.setEdgeValueClass(NullWritable.class);
        doublingPPR2Job.setInputFormat(SequenceFileInputFormat.class);
        doublingPPR2Job.setPartitioner(HashPartitioner.class);
        doublingPPR2Job.setOutputFormat(TextOutputFormat.class);
        doublingPPR2Job.setOutputKeyClass(Text.class);
        doublingPPR2Job.setOutputValueClass(StringWritable.class);

        return doublingPPR2Job;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        finalWriter = new PrintWriter("finalWalks.txt", "UTF-8");
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob doublingPPR2Job = createJob(args, conf);
        doublingPPR2Job.waitForCompletion(true);
        finalWriter.close();
    }

}
