package ir.ac.ut.iis.ppr;

import java.io.*;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 11/8/13
 * Time: 3:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class Summarization {

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        File file = new File("fullhavij.txt");
        FileReader reader = new FileReader(file);
        BufferedReader in = new BufferedReader(reader);
        Scanner scanner = new Scanner(in);

        int noOfLines = 0;
        int R = 0;
        int sumOfLambdas = 0;
        while (scanner.hasNextLine()) {
            noOfLines++;
            String firstLine = scanner.nextLine();
            String walks = firstLine.substring(firstLine.indexOf(" ")+1);
            String[] pieces = walks.split(" AND ");
            R = pieces.length;
            if (noOfLines == 1)
                for (int i = 0; i < R; i++)  {
                    System.out.println("gsgdhd: "+pieces[i]);


                    System.out.println("being added: " + (pieces[i].split(",").length - 1));
                    sumOfLambdas += (pieces[i].split(",").length - 1);
//                sumOfLambdas += (pieces[i].length() + 1) / 2;
//                if (i == R - 1 && noOfLines == 1)
//                    break;
                }
        }
        System.out.println("wwwwwwwwwwwwweeffffff: " + sumOfLambdas);

        double[][] C = new double[noOfLines][noOfLines];

        file = new File("fullhavij.txt");
        reader = new FileReader(file);
        in = new BufferedReader(reader);
        scanner = new Scanner(in);

        for (int i = 0; i < noOfLines; i++) {
            String nextLine = scanner.nextLine();

            int colonInd = nextLine.indexOf(':');

            int uInt = Integer.valueOf(nextLine.substring(0, colonInd));
//            System.out.println("adad: " + uInt);
            String walks = nextLine.substring(colonInd + 2);
//            System.out.println("ffffff: " + walks);
            String[] pieces = walks.split(" AND ");
            for (int j = 0; j < R; j++) {
                String currentWalk = pieces[j];
                String[] hops = pieces[j].split(",");
                for (int k = 1; k < hops.length; k++) {
//                    System.out.println("salam, hop: " + hops[k]);
                    C[uInt][Integer.valueOf(hops[k])] = C[uInt][Integer.valueOf(hops[k])] + 1;
                }
            }
        }

        double[][] pi_hat = new double[noOfLines][noOfLines];
        for (int i = 0; i < noOfLines; i++)
            for (int j = 0; j < noOfLines; j++)
                pi_hat[i][j] = C[i][j] / sumOfLambdas;

        printMatToFile(C, "C.txt");
        printMatToFile(pi_hat, "pi_hat.txt");

        scanner.close();
    }




    public static void printMatToFile(double[][] mat, String fileName) throws FileNotFoundException, UnsupportedEncodingException {

        PrintWriter writer = new PrintWriter(fileName, "UTF-8");
        for (int i = 0; i < mat.length; i++) {
            for (int j = 0; j < mat[i].length; j++) {
//                writer.print(String.valueOf(mat[i][j]));
                writer.printf("%f", mat[i][j]);
                if (j != mat[i].length - 1)
                    writer.print('\t');
            }

            if (i != mat.length - 1)
                writer.print('\n');
        }

        writer.close();
    }
}
