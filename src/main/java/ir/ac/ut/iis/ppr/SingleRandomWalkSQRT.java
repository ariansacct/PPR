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
import java.util.*;

/**
* Created with IntelliJ IDEA.
* User: arian
* Date: 10/2/13
* Time: 8:06 PM
* To change this template use File | Settings | File Templates.
*/
public class SingleRandomWalkSQRT {

    static PrintWriter writer;
    static PrintWriter finalWriter;
    static Map<Text, StringWritable> walks = new HashMap<Text, StringWritable>();


    public static class SingleRandomWalkSQRTVertex extends Vertex<Text, NullWritable, StringWritable> {

        @Override
        public void compute(Iterable<StringWritable> stringWritables) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
            if (this.getSuperstepCount() == 0) {
                String value = new String();
                value = value + this.getVertexID() + ",";     // zaheran injoori too cast-e Text be String moshkeli nadare
                this.setValue(new StringWritable(value));
            }

            else if (this.getSuperstepCount() >= 1) {
                Text currentVertexID = this.getVertexID();
                int i = (int) this.getSuperstepCount();

                String oldValueString = this.getValue().get();
                String[] segments = oldValueString.split(",");
                String lastNodeString = segments[segments.length - 1];
                Text lastNodeText = new Text(lastNodeString);
                SegmentGenerator.SegmentGeneratorVertex lastNodeVertex = SegmentGenerator.MAP.get(lastNodeText);
                StringWritable lastNodeValue = SegmentGenerator.SEGMENTS.get(lastNodeVertex);

                String[] lastNodeSegments = lastNodeValue.get().split("-");
                // eshkale ,, ine ke farz mikoni lastNode hamishe yeraghamie
                String ithSegment = new String(lastNodeSegments[i]);
                int commaIndex = ithSegment.indexOf(',');
                String toBeAppended = new String(lastNodeSegments[i].substring(commaIndex + 1));
                String newValueString = oldValueString + toBeAppended;
                if (i != SegmentGenerator.NO_SEGMENTS - 1)
                    newValueString = newValueString + ",";
                this.setValue(new StringWritable(newValueString));

                if (this.getSuperstepCount() == this.getMaxIteration()) {
                    finalWriter.println("finalWalk: " + this.getValue().get());
                    walks.put(this.getVertexID(), this.getValue());
                }

                Map<SegmentGenerator.SegmentGeneratorVertex,ArrayList<String>> havij =
                        new HashMap<SegmentGenerator.SegmentGeneratorVertex,ArrayList<String>>();

                if (havij.containsKey(SegmentGenerator.MAP.get(this.getVertexID()))) {
                    ArrayList<String> al = havij.get(SegmentGenerator.MAP.get(this.getVertexID()));
                    al.add(this.getValue().get());
                }
                else if (! havij.containsKey(SegmentGenerator.MAP.get(this.getVertexID()))) {
                    ArrayList<String> al = new ArrayList<String>();
                    al.add(this.getValue().get());
                    havij.put(SegmentGenerator.MAP.get(this.getVertexID()), al);
                }
                else
                    System.out.println("Impossible");
            }
        }
    }


    public static class SingleRandomWalkSQRTSeqReader extends VertexInputReader<Text, TextArrayWritable, Text, NullWritable, StringWritable> {
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

        GraphJob singleRandomWalkSQRTJob = new GraphJob(conf, SingleRandomWalkSQRT.class);
        singleRandomWalkSQRTJob.setJobName("SingleRandomWalkSQRT");
        singleRandomWalkSQRTJob.setVertexClass(SingleRandomWalkSQRTVertex.class);
        singleRandomWalkSQRTJob.setInputPath(new Path(args[0]));
        singleRandomWalkSQRTJob.setOutputPath(new Path(args[1]));
        singleRandomWalkSQRTJob.setMaxIteration(SegmentGenerator.NO_SEGMENTS - 1); // felan -1 chon tikeye akhare segmentamoon felan kar nemikone
        singleRandomWalkSQRTJob.set("hama.pagerank.alpha", "0.85");
        singleRandomWalkSQRTJob.set("hama.graph.self.ref", "true");
        singleRandomWalkSQRTJob.set("hama.graph.max.convergence.error", "0.001");
        singleRandomWalkSQRTJob.setVertexInputReaderClass(SingleRandomWalkSQRTSeqReader.class);
        singleRandomWalkSQRTJob.setVertexIDClass(Text.class);
        singleRandomWalkSQRTJob.setVertexValueClass(StringWritable.class);
        singleRandomWalkSQRTJob.setEdgeValueClass(NullWritable.class);
        singleRandomWalkSQRTJob.setInputFormat(SequenceFileInputFormat.class);
        singleRandomWalkSQRTJob.setPartitioner(HashPartitioner.class);
        singleRandomWalkSQRTJob.setOutputFormat(TextOutputFormat.class);
        singleRandomWalkSQRTJob.setOutputKeyClass(Text.class);
        singleRandomWalkSQRTJob.setOutputValueClass(StringWritable.class);

        return singleRandomWalkSQRTJob;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        finalWriter = new PrintWriter("finalWalks.txt", "UTF-8");
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob singleRandomWalkSQRTJob = createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (singleRandomWalkSQRTJob.waitForCompletion(true))
            System.out.println("singleRandomWalkSQRTJob Finished in "
                    + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        finalWriter.close();

    }

}
