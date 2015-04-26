package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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


public class ReduceSideJoin extends Configured implements Tool {

  private Path outputDir;
  private Path inputPath;
  private int numReducers;

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
  
    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(new Path(args[2]))){
       fs.delete(new Path(args[2]),true);
    }
    
    Job job = new Job(conf, "reducesidejoin"); // TODO: define new job instead of null using conf

    // TODO: set job input format
    job.setInputFormatClass(TextInputFormat.class);
    
    // TODO: set map class and the map output key and value classes
    job.setMapperClass(ReduceSlideJoinsMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TextPair.class);
    
    //job.setPartitionerClass(ReduceSlideJoinsPartitioner.class);
    
    // TODO: set reduce class and the reduce output key and value classes
    job.setReducerClass(ReduceSlideJoinsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    // TODO: set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
    
    // TODO: add the input file as job input (from HDFS) to the variable
    //       inputFile
    FileInputFormat.addInputPath(job, inputPath);
    
    // TODO: set the output path for the job results (to HDFS) to the variable
    //       outputPath
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // TODO: set the number of reducers using variable numberReducers
    job.setNumReduceTasks(numReducers);
    
    // TODO: set the jar class
    job.setJarByClass(ReduceSideJoin.class);

    return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
  }

  public ReduceSideJoin(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
    System.exit(res);
  }
  
  public static class ReduceSlideJoinsMapper extends Mapper<LongWritable, Text, Text, TextPair>{
	  private final static Text zero = new Text("0");
	  private final static Text one = new Text("1");
	@Override
	protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		String[] words = value.toString().split("\t");
		
		context.write(new Text(words[0]), new TextPair(zero, new Text(words[1])));
		context.write(new Text(words[1]), new TextPair(one, new Text(words[0])));
	}
  }
  
  public static class ReduceSlideJoinsReducer extends Reducer<Text, // TODO: change Object to input key type
  															TextPair, // TODO: change Object to input value type
  															Text, // TODO: change Object to output key type
  															Text> {
	Set<TextPair> filter = new HashSet<TextPair>(); 
	private Text ro = new Text("");
	private Text lo = new Text("");
	  @Override
	protected void reduce(Text key, Iterable<TextPair> values,
						Context arg2)
			throws IOException, InterruptedException {
		  Iterator<TextPair> iter = values.iterator();
		  List<String> ref = new ArrayList<String>();
		  List<String> log = new ArrayList<String>();
			
		  TextPair value;
		  while (iter.hasNext()){
			  value = iter.next();
			  int type = Integer.parseInt(value.getFirst().toString());
			  if (type == 1){
				  ref.add(value.getSecond().toString());
			  }else{
				  log.add(value.getSecond().toString());
			  }
		  }
		  
		 // For both one-to-one, one-to-many, many-to-many join 
		  for(String r: ref)
				for(String l: log){
					if (r.compareTo(l) == 0)
						continue;
					
					ro.set(r);
					lo.set(l);
					
					arg2.write(ro, lo);
				}
	}
	  
	  @Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	}
  }
}