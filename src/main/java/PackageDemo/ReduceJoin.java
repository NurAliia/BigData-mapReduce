package PackageDemo;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
    public static class FirstTestMapper extends Mapper <LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] words = record.split(",");
            for (String word: words)
            {
                context.write(new Text(word), new Text("First"));
            }
        }
    }

    public static class SecondTestMapper extends Mapper <LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String record = value.toString();
            String[] words = record.split(",");
            for (String word: words)
            {
                context.write(new Text(word), new Text("Second"));
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            Text second = new Text("Second");
            Boolean result = StreamSupport.stream(values.spliterator(), false)
                    .anyMatch(name -> second.equals(name));

            if (!result)
            {
                context.write(key, new Text("unique"));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(ReduceJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FirstTestMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SecondTestMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
