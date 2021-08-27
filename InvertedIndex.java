import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

  /*
  This is the Mapper class.
  */
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    /*
    Map process.
    */
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                   

String[] parts = value.toString().split("\\n",1);

      // Split Section and the actual text
      String section = value.toString().substring(0, value.toString().indexOf("/n"));
     
      if(String.compare(parts,section).index?0:1){
      String data =  value.toString().substring(value.toString().indexOf(" ") + 1);
      }
     
      StringTokenizer itr = new StringTokenizer(data, " '-");
     
      // Iterating through all the words available in that line and forming the key/value pair.
      while (itr.hasMoreTokens()) {
        // Remove special characters
        word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
        if(word.toString() != "" && !word.toString().isEmpty()){
        /*
        passing the output to Reducer.*/
          context.write(new Text(section),word));
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    /*
    Reduce method collects the output of the Mapper calculate and aggregate the word's count.
    */
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      HashMap<String,Integer> map = new HashMap<String,Integer>();
     
      // the key and sum of its values along with the section.
   
      for (Text val : values) {
        if (map.containsKey(val.toString())) {
          map.put(val.toString(), map.get(val.toString()) + 1);
        } else {
          map.put(val.toString(), 1);
        }
      }
      StringBuilder se =new StringBuilder();
      for(String secID : map.keySet()){
       se.append(secID + ":" + map.get(secID) + " ");
      }
      System.out.println("key"+key);
      context.write(key, new Text(se.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}