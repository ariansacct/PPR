package ir.ac.ut.iis.ppr;

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
import java.util.Map;

/**
* Created with IntelliJ IDEA.
* User: arian
* Date: 10/24/13
* Time: 10:18 PM
* To change this template use File | Settings | File Templates.
*/
public class Doubling {
//    static int LAMBDA = SingleRandomWalk.LAMBDA;
//    static int THETA = SingleRandomWalk.THETA;
//    static int NO_SEGMENTS = SingleRandomWalk.NO_SEGMENTS;
//    static int ETA = LAMBDA / THETA + 1;
//    static Map<Text, SingleRandomWalk.SingleRandomWalkVertex> map;
//    static Map<SingleRandomWalk.SingleRandomWalkVertex, StringWritable> genSegResults;
//    static ArrayList<StringWritable> arrayList = new ArrayList<StringWritable>();
//
//    static PrintWriter writer;
//
//    public static class DoublingVertex extends Vertex<Text, NullWritable, StringWritable> {
//        @Override
//        public void compute(Iterable<StringWritable> stringWritables) throws IOException {
//            if (this.getSuperstepCount() == 0) {
//                //  format: "eta: , , - , , - , , "
//                String value = new String(ETA + ":");
//                for (int i = 0; i < ETA; i++) {
//                    value = value + this.getVertexID().toString() + ",";
//                    if (i != ETA - 1)
//                        value = value + "-";
//                }
//
//                this.setValue(new StringWritable(value));
//            }
//
//            else if (this.getSuperstepCount() >= 1) {
//                String thisVertexValue = this.getValue().get();
//                String thisVertexValueStripped = thisVertexValue.substring(thisVertexValue.indexOf(':') + 1);
//                while (ETA > 1) {   //  ghalate ehtemalan, be khatere shifte andis
//                    int eta_prime = (ETA + 1) / 2;
//                    for (int i = 0; i < eta_prime; i++) {
//                        String temp;
//                        if (i == (ETA + 1) / 2) {
//                            String nemidoonam = this.getValue().get();  //  nemidoonam bayad eta_prime bashe (pseudoro bebin)
////                    String nemidoonam2 = s[u,i].lastNode;
//                            // hala parse-e value
//                            String stripped = nemidoonam.substring(nemidoonam.indexOf(':') + 1);
//                            String[] segments = stripped.split("-");
//                            String[] hops = segments[i].split(",");
//                            String lastNodeStr = hops[hops.length - 1];
//                            String nemidoonam2 = lastNodeStr;
//
//                            // continue;   ino chi karesh konam?!
//                        }
//                        String vStr = nemidoonam2;
//                    }
//
//                }
//            }
//        }
//    }
//
//    public static class DoublingSeqReader extends VertexInputReader<Text, TextArrayWritable, Text, NullWritable, StringWritable> {
//        @Override
//        public boolean parseVertex(Text key, TextArrayWritable value, Vertex<Text, NullWritable, StringWritable> vertex) throws Exception {
//            vertex.setVertexID(key);
//
//            for (Writable v : value.get()) {
//                vertex.addEdge(new Edge<Text, NullWritable>((Text) v, null));
//            }
//
//            return true;
//        }
//    }
//
//    public static GraphJob createJob(String[] args, HamaConfiguration conf)
//            throws IOException {
//        GraphJob doublingJob = new GraphJob(conf, Doubling.class);
//        doublingJob.setJobName("Doubling");
//
//        doublingJob.setVertexClass(DoublingVertex.class);
//        doublingJob.setInputPath(new Path(args[0]));
//        doublingJob.setOutputPath(new Path(args[1]));
//
//        // set the defaults
//        doublingJob.setMaxIteration(1);
//        doublingJob.set("hama.pagerank.alpha", "0.85");
//        // reference vertices to itself, because we don't have a dangling node
//        // contribution here
//        doublingJob.set("hama.graph.self.ref", "true");
//        doublingJob.set("hama.graph.max.convergence.error", "0.001");
//
////        if (args.length == 3) {
////            singleRandomWalkJob.setNumBspTask(Integer.parseInt(args[2]));
////        }
//
//        // error
////        singleRandomWalkJob.setAggregatorClass(AverageAggregator.class);
//
//        // Vertex reader
//        doublingJob.setVertexInputReaderClass(DoublingSeqReader.class);
//
//        doublingJob.setVertexIDClass(Text.class);
//        doublingJob.setVertexValueClass(StringWritable.class);
//        doublingJob.setEdgeValueClass(NullWritable.class);
//
//        doublingJob.setInputFormat(SequenceFileInputFormat.class);
//
//        doublingJob.setPartitioner(HashPartitioner.class);
//        doublingJob.setOutputFormat(TextOutputFormat.class);
//        doublingJob.setOutputKeyClass(Text.class);
//        doublingJob.setOutputValueClass(StringWritable.class);
//        return doublingJob;
//    }
}
