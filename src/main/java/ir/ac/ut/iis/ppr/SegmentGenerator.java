package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 9/27/13
 * Time: 11:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class SegmentGenerator {

    static int LAMBDA;
    static int THETA;
    static int NO_SEGMENTS;
    static Map<SegmentGeneratorVertex, StringWritable> SEGMENTS = new HashMap<SegmentGeneratorVertex, StringWritable>();
    static Map<Text, SegmentGeneratorVertex> MAP = new HashMap<Text, SegmentGeneratorVertex>();
    static PrintWriter writer;

//    SegmentGeneratorVertex sgv = new SegmentGeneratorVertex();

    public static class SegmentGeneratorVertex extends Vertex<Text, NullWritable, StringWritable> {

        @Override
        public void compute(Iterable<StringWritable> stringWritables) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
            genSeg();
        }

        public void genSeg() throws FileNotFoundException, UnsupportedEncodingException {
            if (this.getSuperstepCount() == 0) {

                if (! MAP.containsKey(this.getVertexID()))
                    MAP.put(this.getVertexID(), this);

                String value = new String();
                for (int i = 0; i < NO_SEGMENTS; i++) {
                    value = value + this.getVertexID();
                    if (i != NO_SEGMENTS - 1)
                        value = value + "-";
                }

                this.setValue(new StringWritable(value));
            }


            else if (this.getSuperstepCount() >= 1) {

                String oldValue = this.getValue().get();
                String[] segments = oldValue.split("-");

                for (int i = 0; i < segments.length; i++) {

                    if ((i  == segments.length - 1) && ((LAMBDA % THETA) > 0) && ((LAMBDA % THETA) < this.getSuperstepCount() + 1))
                        break;  // the last segment

                    String segment = segments[i];
                    String[] hops = segment.split(",");

                    Text lastHopID = new Text(hops[hops.length - 1]);
                    Vertex lastHop = MAP.get(lastHopID);

                    MapPrinter.print(MAP, "map.txt");

                    List<Edge<Text,NullWritable>> outedges = lastHop.getEdges();
                    Random generator = new Random();
                    int randomIndex = generator.nextInt(outedges.size() - 1);   // divoone be khodesh outedge mide!
                    Edge randomEdge = outedges.get(randomIndex);
                    Text randomEdgeID = (Text) randomEdge.getDestinationVertexID();
                    segment = segment + ",";
                    segment = segment + randomEdgeID.toString();
                    segments[i] = segment;
                }
                String newValue = new String();
                for (int i = 0; i < segments.length; i++) {
                    newValue = newValue + segments[i];
                    if (i != segments.length - 1)
                        newValue = newValue + "-";
                }
                this.setValue(new StringWritable(newValue));


                if (this.getSuperstepCount() == this.getMaxIteration()) {
                    SEGMENTS.put(this, this.getValue());
                }

            }
        }
    }

    public static class SegmentGeneratorSeqReader extends VertexInputReader<Text, TextArrayWritable, Text, NullWritable, StringWritable> {
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
        GraphJob segmentGeneratorJob = new GraphJob(conf, SegmentGenerator.class);
        segmentGeneratorJob.setJobName("SegmentGenerator");
        segmentGeneratorJob.setVertexClass(SegmentGeneratorVertex.class);
        segmentGeneratorJob.setInputPath(new Path(args[0]));
        segmentGeneratorJob.setOutputPath(new Path(args[1]));
        segmentGeneratorJob.setMaxIteration(THETA);
        segmentGeneratorJob.set("hama.graph.self.ref", "true");
        segmentGeneratorJob.set("hama.graph.max.convergence.error", "0.001");

        if (args.length == 3) {
            segmentGeneratorJob.setNumBspTask(Integer.parseInt(args[2]));
        }

        segmentGeneratorJob.setVertexInputReaderClass(SegmentGeneratorSeqReader.class);
        segmentGeneratorJob.setVertexIDClass(Text.class);
        segmentGeneratorJob.setVertexValueClass(StringWritable.class);
        segmentGeneratorJob.setEdgeValueClass(NullWritable.class);
        segmentGeneratorJob.setInputFormat(SequenceFileInputFormat.class);
        segmentGeneratorJob.setPartitioner(HashPartitioner.class);
        segmentGeneratorJob.setOutputFormat(TextOutputFormat.class);
        segmentGeneratorJob.setOutputKeyClass(Text.class);
        segmentGeneratorJob.setOutputValueClass(StringWritable.class);

        return segmentGeneratorJob;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob segmentGeneratorJob = createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (segmentGeneratorJob.waitForCompletion(true))
            System.out.println("segmentGeneratorJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
}