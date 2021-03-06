package ir.ac.ut.iis.ppr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.examples.util.SymmetricMatrixGen;
import org.junit.*;

import java.io.PrintWriter;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 11/25/13
 * Time: 1:31 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.examples.util.SymmetricMatrixGen;
import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 10/16/13
 * Time: 10:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class MyGraphGeneratorTest {
    protected static Log LOG = LogFactory.getLog(MyGraphGeneratorTest.class);
    private static String TEST_OUTPUT = "/home/arian/IdeaProjects/MyProject/fullinput";

    @org.junit.Test
    public void testGraphGenerator() throws Exception {
        PrintWriter printWriter = new PrintWriter("/home/arian/IdeaProjects/MyProject/fullgraph.txt", "UTF-8");

        Configuration conf = new Configuration();

        MyGraphGenerator.main(new String[]{"3", "10", TEST_OUTPUT, "1"});
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] globStatus = fs.globStatus(new Path(TEST_OUTPUT + "/part-*"));
        for (FileStatus fts : globStatus) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, fts.getPath(),
                    conf);
            Text key = new Text();
            TextArrayWritable value = new TextArrayWritable();

            while (reader.next(key, value)) {
                String values = "";
                for (Writable v : value.get()) {
                    values += v.toString() + " ";
                }
                LOG.info(fts.getPath() + ": " + key.toString() + " | " + values);

                printWriter.write(key.toString() + " | " + values);
                printWriter.write('\n');
            }
            reader.close();
        }

        printWriter.close();

//        fs.delete(new Path(TEST_OUTPUT), true);
    }
}

