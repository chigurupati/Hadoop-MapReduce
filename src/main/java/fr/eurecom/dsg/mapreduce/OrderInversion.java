package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class OrderInversion extends Configured implements Tool {

    private final static String ASTERISK = "\0";
    
    public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {
    
        @Override
        public int getPartition(TextPair key, IntWritable value, int numPartitions) {
        // TODO: implement getPartition such that pairs with the same first element
        // will go to the same reducer. You can use toUnsighed as utility.
            int hash = 0;
            for (byte b: key.getFirst().toString().getBytes()) {
                hash += toUnsigned(b);
            }
            return hash % numPartitions;
        }
        
        /**
        * toUnsigned(10) = 10
        * toUnsigned(-1) = 2147483647
        *
        * @param val Value to convert
        * @return the unsigned number with the same bits of val
        * */
        public static int toUnsigned(int val) {
            return val & Integer.MAX_VALUE;
        }
    }
    
    public static class PairMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            // TODO: implement the map method
            
             //List<StripesTuple> results = new ArrayList<StripesTuple>();
            //StringTokenizer lineTokenizer = new StringTokenizer(value, ".!?");
           // while (lineTokenizer.hasMoreElements()) {
           // String line = lineTokenizer.nextToken();
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");
            List<String> wordList = new ArrayList<String>();
            while (tokenizer.hasMoreElements()) {
                String rawWord = tokenizer.nextToken();
                rawWord = rawWord.toLowerCase().replaceAll("[,;\\.:“”ñ—’]", "");
                wordList.add(rawWord);
            }
            
             for (String word: wordList) {
                int count = 0;
                for (String pairWord: wordList) {
                    if (!word.equals(pairWord)){
                        count += 1;
                        TextPair pair = new TextPair(word, pairWord);
                        IntWritable one = new IntWritable(1);
                        context.write(pair, one);
                    }
                }
                context.write(new TextPair(word, ASTERISK), new IntWritable(count));
            }
       // }
        }
    }
    public static class PairReducer extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {
        // TODO: implement the reduce method
        
        private final Map<String, Integer> totalCounts = new HashMap<String, Integer>();
        
        @Override
        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
            if (key.getSecond().toString().equals(ASTERISK)) {
                
                int totalCount = 0;
                for (IntWritable value: values) {
                    totalCount += value.get();
                }
                
                totalCounts.put(key.getFirst().toString(), totalCount);
                
            }else{
                
                int total = totalCounts.get(key.getFirst().toString());
                int wordsSeen = 0;
                for (IntWritable word: values) {
                    wordsSeen += word.get();
                }
                double frequency = ((double) wordsSeen) / total;
                context.write(key, new DoubleWritable(frequency));
            }
        }
    }
    
    private int numReducers;
    private Path inputPath;
    private Path outputDir;
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
       
        Job job = new Job(conf);
        job.setJobName("Order Inversion");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PairMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setNumReduceTasks(numReducers);
        job.setJarByClass(Pair.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
    OrderInversion(String[] args) {
        if (args.length != 3) {
        System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
        System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }
}