package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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


public class Pair extends Configured implements Tool {

    public static class PairMapper
            extends Mapper<LongWritable, // TODO: change Object to input key type
            Text, // TODO: change Object to input value type
            TextPair, // TODO: change Object to output key type
            IntWritable> { // TODO: change Object to output value type
        // TODO: implement mapper

        private final TextPair pair = new TextPair();
        private final IntWritable one = new IntWritable(1);
        private int window = 2;
        @Override
        public void setup(Context context) {
            window = context.getConfiguration().getInt("window", 2);
        }
        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException,
                InterruptedException {
            String text = line.toString();
            String[] terms = text.split(" ");
            StringTokenizer st  = new StringTokenizer(line.toString()," ");
            int TotalWords = st.countTokens();

            List<String> words = new ArrayList<String>();
            while(st.hasMoreTokens()){
                String token = st.nextToken();
                token = token.toLowerCase().replaceAll("[\\(\\),;\\.:\"\'“”—’]", "");
                words.add(token);
            }
            for (String word:words){
                for(String myPair : words){
                    if(!word.equals(myPair)){
                        pair.set(new Text(word), new Text(myPair));
                        context.write(pair, one);
                    }
                }
            }
            /*
            window = terms.length;
            for (int i = 0; i < terms.length; i++) {
                String term = terms[i];
                // skip empty tokens
                if (term.length() == 0)
                    continue;
                for (int j = i - window; j < i + window + 1; j++) {
                    if (j == i || j < 0)
                        continue;
                    if (j >= terms.length)
                        break;
// skip empty tokens
                    if(term.equals(terms[j])){
                        continue;
                    }
                    if (terms[j].length() == 0)
                        continue;
                    pair.set(new Text(term), new Text(terms[j]));
                    context.write(pair, one);
                }
            } */
        }
    }

    public static class PairReducer
            extends Reducer<TextPair, // TODO: change Object to input key type
            IntWritable, // TODO: change Object to input value type
            TextPair, // TODO: change Object to output key type
            IntWritable> { // TODO: change Object to output value type
        // TODO: implement reducer

        private final static IntWritable SumValue = new IntWritable();
        @Override
        public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
           /* Iterator <IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            */
            int accumulator = 0;
            for (IntWritable value : values) {
                accumulator += value.get();
            }

            SumValue.set(accumulator);
            context.write(key, SumValue);
        }
    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        //Job job = null; // TODO: define new job instead of null using conf
        Job job = new Job(conf,"Pairs");

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);

        // TODO: set map class and the map output key and value classes
        job.setMapperClass(PairMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // TODO: add the input file as job input (from HDFS) to the variable

        FileInputFormat.addInputPath(job, new Path(args[1]));

        //       inputPath
        // TODO: set the output path for the job results (to HDFS) to the variable
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));

        // TODO: set the jar class
        job.setJarByClass(Pair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }
}
