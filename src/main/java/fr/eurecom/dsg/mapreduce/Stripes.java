package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;


public class Stripes extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        //Job job = null; // TODO: define new job instead of null using conf
        Job job = new Job(conf,"Stripes");

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // TODO: set map class and the map output key and value classes
        job.setMapperClass(StripesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringToIntMapWritable.class);

        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringToIntMapWritable.class);


        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // TODO: add the input file as job input (from HDFS) to the variable

        FileInputFormat.addInputPath(job, new Path(args[1]));

        //       inputPath
        // TODO: set the output path for the job results (to HDFS) to the variable
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //       outputPath

        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));

        // TODO: set the jar class
        job.setJarByClass(Stripes.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Stripes (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
        System.exit(res);
    }
}

class StripesMapper
        extends Mapper<LongWritable,   // TODO: change Object to input key type
        Text,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type

    private final Text outputKey = new Text();
    
    @Override
    public void map(LongWritable key, // TODO: change Object to input key type
                    Text value, // TODO: change Object to input value type
                    Context context)
            throws java.io.IOException, InterruptedException {

        // TODO: implement map method
        
        //List<StripesTuple> results = new ArrayList<StripesTuple>();
        //StringTokenizer lineTokenizer = new StringTokenizer(value, ".!?");
       // while (lineTokenizer.hasMoreElements()) {
       // String line = lineTokenizer.nextToken();
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");
        List<String> wordList = new ArrayList<>();
        while (tokenizer.hasMoreElements()) {
            String rawWord = tokenizer.nextToken();
            rawWord = rawWord.toLowerCase().replaceAll("[,;\\.:“”ñ—’]", "");
            wordList.add(rawWord);
        }
        
        for (String word: wordList) {
            Map<String, Integer> counts = new HashMap<>();
            for (String pairWord: wordList) {
                if (!word.equals(pairWord)){
                    if (counts.containsKey(pairWord)) {
                        counts.put(pairWord, counts.get(pairWord) + 1);
                    } else {
                        counts.put(pairWord, 1);
                    }
                }       
            }
            StringToIntMapWritable pair = new StringToIntMapWritable(counts);
            outputKey.set(word);
            context.write(outputKey, pair);
        }
       // }
    }
}

class StripesReducer
        extends Reducer<Text,   // TODO: change Object to input key type
        StringToIntMapWritable,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type
    @Override
    public void reduce(Text key, // TODO: change Object to input key type
                       Iterable<StringToIntMapWritable> values, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method
        Map<String, Integer> bigMap = new HashMap<String, Integer>();
        
        for (StringToIntMapWritable smallMap: values) { //for each strip
            for (String word: smallMap.counts.keySet()) { // for each key(word) in the strip  
                if (bigMap.containsKey(word)) { // sum all the same keys to form a row of strip for the main word (key or with which all the inner keys occoccure).
                    bigMap.put(word, bigMap.get(word) + smallMap.counts.get(word));
                }else{
                    bigMap.put(word, smallMap.counts.get(word));
                }
            }
        }
        context.write(key, new StringToIntMapWritable(bigMap));
    }
}