package fr.eurecom.dsg.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedCacheJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputFile;
    private Path inputTinyFile;
    private int numReducers;
    
    public DistributedCacheJoin(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: DistributedCacheJoin <num_reducers> " +
            "<input_tiny_file> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputTinyFile = new Path(args[1]);
        this.inputFile = new Path(args[2]);
        this.outputDir = new Path(args[3]);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJobName("Distributed Cache Join");
        DistributedCache.addCacheFile(inputTinyFile.toUri(), job.getConfiguration());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(DCJMapper.class);
        job.setReducerClass(DCJReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, inputFile);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setNumReduceTasks(numReducers);
        job.setJarByClass(DistributedCacheJoin.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
        new DistributedCacheJoin(args),
        args);
        System.exit(res);
    }
}
// TODO: implement mapper

class DCJMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    Set<String> ignorePatterns = new HashSet<String>();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable one = new IntWritable(1);
        for (String word: value.toString().split("\\s+")) {
            word = word.toLowerCase().replaceAll("[\\(\\),;\\.:\"\'“”—’]", "");
            if (!ignorePatterns.contains(word)) {
                context.write(new Text(word), one);
            }
        }
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        
        super.setup(context);
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        
        for (Path p: cacheFiles) {
            FileReader fr = new FileReader(new File(p.toString()));
            BufferedReader br = new BufferedReader(fr);
            String line;
            try {
                while ( (line = br.readLine()) != null) {
                    line = line.trim();
                    ignorePatterns.add(line);
                }
            } finally {
                br.close();
            }
        }
    }
}
   
class DCJReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable currentInt: values) {
            sum += currentInt.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
