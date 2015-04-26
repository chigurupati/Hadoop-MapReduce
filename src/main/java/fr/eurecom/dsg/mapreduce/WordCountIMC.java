package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 *
 */
public class WordCountIMC extends Configured implements Tool {


    private int numReducers;
    private Path inputPath;
    private Path outputDir;




    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        //Job job = null; // TODO: define new job instead of null using conf
        Job job = new Job(conf, "WordCountIMC");

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // TODO: set map class and the map output key and value classes
        job.setMapperClass(WCIMCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(WCIMCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


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
        job.setJarByClass(WordCountIMC.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public WordCountIMC (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
        System.exit(res);
    }
}

class WCIMCMapper extends Mapper<LongWritable, // TODO: change Object to input key
        // type
        Text, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    private Map<String,Integer> associativeArray = new HashMap<String,Integer>();

    public Map<String, Integer> getAssociativeArray() {
        return associativeArray;
    }
    private static final int FLUSH_SIZE = 1000;
    public void setAssociativeArray(Map<String, Integer> associativeArray) {
        this.associativeArray = associativeArray;
    }

    @Override
    protected void map(LongWritable key, // TODO: change Object to input key type
                       Text value, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {
        Map<String,Integer> arrayTemp = getAssociativeArray();

        // * TODO: implement the map method (use context.write to emit results). Use
        // the in-memory combiner technique
        StringTokenizer st  = new StringTokenizer(value.toString());
        while(st.hasMoreTokens()){
            //context.write(new Text(st.nextToken()),new IntWritable(1));
            //arrayTemp[st.nextToken()] = arrayTemp[st.nextToken()]+1;
            String token = st.nextToken();
            if(arrayTemp.containsKey(token)) {
                int total = arrayTemp.get(token) + 1;
                arrayTemp.put(token, total);
            } else {
                arrayTemp.put(token, 1);
            }
        }
        flushMap(context, false);
    }

    void flushMap(Context context, boolean force) throws IOException, InterruptedException {
        Map<String, Integer> map = getAssociativeArray();
        if(!force) {
            int size = map.size();
            if(size < FLUSH_SIZE)
                return;
        }

        if(map != null){
            Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
            while(it.hasNext()){
                String key = it.next().getKey();
                //map.get(key);
                int count = map.get(key).intValue();
                context.write(new Text(key),new IntWritable(count));
            }
        }
        map.clear();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        flushMap(context, true);
    }
}

class WCIMCReducer extends Reducer<Text, // TODO: change Object to input key
        // type
        IntWritable, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    @Override
    protected void reduce(Text key, // TODO: change Object to input key type
                          Iterable<IntWritable> values, // TODO: change Object to input value type
                          Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method (use context.write to emit results)
        int count = 0;
        for(IntWritable val: values){
            count = count + val.get();
        }
        context.write(key,new IntWritable(count));
    }
}
