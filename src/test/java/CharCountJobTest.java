import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import PackageDemo.CharCount.MapForCharCount;
import PackageDemo.CharCount.ReduceForCharCount;

import org.junit.Test;
import org.junit.Before;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;

public class CharCountJobTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
//        System.setProperty("hadoop.home.dir", "C:/BigData/winutils/winutils.exe");

        final MapForCharCount mapper = new MapForCharCount();
        final ReduceForCharCount reducer = new ReduceForCharCount();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text(
                "H"));
        mapDriver.withOutput(new Text("H"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("b"), values);
        reduceDriver.withOutput(new Text("b"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text("bb"));
        mapReduceDriver.withOutput(new Text("B"), new IntWritable(2));
        // Will be failure
        // mapReduceDriver.withOutput(new Text("B"), new IntWritable(4));
        mapReduceDriver.runTest();
    }
}
