package ir.ac.ut.iis.ppr;

import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 2/4/14
 * Time: 11:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class MapPrinter {

    public static void print(Map<Text, SegmentGenerator.SegmentGeneratorVertex> m, String filename) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter printWriter = new PrintWriter(filename, "UTF-8");
        Set ks = m.keySet();
        Iterator it = ks.iterator();
        while (it.hasNext()) {
            Object current = it.next();
            printWriter.println(current + ": " + m.get(current));
        }
        printWriter.close();
    }
}
