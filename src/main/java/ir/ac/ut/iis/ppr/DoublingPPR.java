//package ir.ac.ut.iis.ppr;
//
//import org.apache.hadoop.io.Text;
//
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.*;
//
///**
//* Created with IntelliJ IDEA.
//* User: arian
//* Date: 11/3/13
//* Time: 10:28 PM
//* To change this template use File | Settings | File Templates.
//*/
//public class DoublingPPR {
//
//    static int L;
//    static double EPSILON;
//    static int R = (int) (L * EPSILON);
//    static Map<SegmentGenerator.SegmentGeneratorVertex, ArrayList<String>> fingerprints;
//
//    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
////        fingerprints = new HashMap<SegmentGenerator.SegmentGeneratorVertex, String[]>();
////        fingerprints = SingleRandomWalkSQRT.allWalks;   // at wrong place
////        int L = 100;
////        double epsilon = 0.15;
////        int R = (int) (L * epsilon);
//
////        GeometricDistribution geometricDist = new GeometricDistribution(epsilon);
//        int[] lambdas = new int[R];
//
//        Map<Text, ArrayList<StringWritable>> allwalks =
//                new HashMap<Text, ArrayList<StringWritable>>();
//
////        String[] fingerprints = new String[R];
//        for (int i = 0; i < R; i++) {
//
////            fingerprints = SingleRandomWalkSQRT.allWalks;
//
//            lambdas[i] = StdRandom.geometric(EPSILON);
//            System.out.println("Lambda: " + lambdas[i]);
//            SegmentGenerator.LAMBDA = lambdas[i];
//            SegmentGenerator.THETA = 1;
//            SegmentGenerator.NO_SEGMENTS = (int) ((double) SegmentGenerator.LAMBDA / (double) SegmentGenerator.THETA + 1);
////            SegmentGenerator.NO_SEGMENTS = SegmentGenerator.LAMBDA / SegmentGenerator.THETA + 1;
////            SegmentGenerator.NO_SEGMENTS = SegmentGenerator.LAMBDA / SegmentGenerator.THETA;
////            SingleRandomWalkSQRT.LAMBDA = SegmentGenerator.LAMBDA;
////            SingleRandomWalkSQRT.THETA = SegmentGenerator.THETA;
////            SingleRandomWalkSQRT.NO_SEGMENTS = SegmentGenerator.NO_SEGMENTS;
//            SingleRandomWalkSQRT.main(args);
//            Map<Text, StringWritable> walksForThisSuperStep =
//                    SingleRandomWalkSQRT.walksForThisSuperStep;
//
//            for (Map.Entry<Text, StringWritable> entry : walksForThisSuperStep.entrySet()) {
//                ArrayList<StringWritable> arrayList = allwalks.get(entry.getKey());
//                if (arrayList == null)
//                    arrayList = new ArrayList<StringWritable>();
//                arrayList.add(entry.getValue());
//                allwalks.put(entry.getKey(), arrayList);
//            }
//
//        }
//
//        PrintWriter printWriter = new PrintWriter("fullhavij23.txt", "UTF-8");
//
//        for (Map.Entry<Text, ArrayList<StringWritable>> entry : allwalks.entrySet()) {
//            printWriter.write(entry.getKey().toString() + ": ");
//            ArrayList<StringWritable> arrayList = entry.getValue();
//            for (StringWritable sw : arrayList) {
//                printWriter.write(sw.get());
//                printWriter.write(" AND ");
//            }
//            printWriter.write('\n');
//        }
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
////        Set<Map.Entry<SegmentGenerator.SegmentGeneratorVertex, ArrayList<String>>> hey =
////                fingerprints.entrySet();
////        Iterator<Map.Entry<SegmentGenerator.SegmentGeneratorVertex, ArrayList<String>>> iterator =
////                hey.iterator();
////        while (iterator.hasNext()) {
////            Map.Entry<SegmentGenerator.SegmentGeneratorVertex,ArrayList<String>> current = iterator.next();
////            ArrayList<String> strings = current.getValue();
////            printWriter.write(current.getKey().getVertexID().toString() + ": ");
////            for (int i = 0; i < strings.size(); i++) {
////                printWriter.write(strings.get(i));
////                if (i != strings.size() - 1)
////                    printWriter.write(" AND ");
////
////            }
////
////            printWriter.write('\n');
////        }
//
//        printWriter.close();
//
//
//
//
//
//
//
//
////        int[][] C = new int[50][50]
//
//    }
//
//}
