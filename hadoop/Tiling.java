/* Java includes */ 
import java.io.IOException;
//import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.math.BigInteger;
import java.io.DataOutput;
import java.io.DataInput;

/* Hadoop includes */
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.RecordReader;

class Edge implements WritableComparable {
  int[] src;
  int[] dst;
  int n;

  public Edge(){
  }

  public Edge(int[] src, int[] dst){
    this.n = src.length;
    this.src = src;
    this.dst = dst;
  }

  public int compareTo(Object input){
    // TODO: Handle case when input is not of type Edge
    Edge edge2 = (Edge)input;
    for (int i = 0; i < this.n; i++)
      if (this.dst[i] < edge2.dst[i])
        return -1;
      else if (this.dst[i] > edge2.dst[i])
        return 1;
    for (int i = 0; i < this.n; i++)
      if (this.src[i] < edge2.src[i])
        return -1;
      else if (this.src[i] > edge2.src[i])
        return 1;
    return 0;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(n);
    for (int i = 0; i < n; i++)
      out.writeInt(src[i]);
    for (int i = 0; i < n; i++)
      out.writeInt(dst[i]);
    return;
  }

  public void readFields(DataInput in) throws IOException {
    n = in.readInt();
    src = new int[n];
    dst = new int[n];

    for (int i = 0; i < n; i++)
      src[i] = in.readInt();
    for (int i = 0; i < n; i++)
      dst[i] = in.readInt();
    return;
  }

  public String toString(){
    String ans = "{[";
    for (int i = 0; i < n; i++)
      ans = ans + Integer.toString(src[i]) + ",";
    ans = ans + "] -> [";
    for (int i = 0; i < n; i++)
      ans = ans + Integer.toString(dst[i]) + ",";
    ans = ans + "]}";
    return ans; 
  }

}


class BigList implements Writable {
  ArrayList<BigInteger> data;

  public BigList(int n){
    data = new ArrayList<BigInteger>(n);
    for (int i = 0; i < n; i++)
      data.add(i,BigInteger.valueOf(0));
  }

  public BigList(BigList list){
    data = new ArrayList<BigInteger>(list.data.size());
    for (int i = 0; i < list.data.size(); i++)
      data.add(i,list.data.get(i));
  }

  public BigList(){
    new BigList(1);
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(data.size());
    for (int i = 0; i < data.size(); i++)
      out.writeBytes(data.get(i).toString()+"\n");
    return;
  }

  public void readFields(DataInput in) throws IOException {
    int n = in.readInt();
    data = new ArrayList<BigInteger>(n);
    for (int i = 0; i < n; i++)
      data.add(i, new BigInteger(in.readLine()));
    return;
  }

  public String toString(){
    String ans = "[";
    for (int i = 0; i < data.size(); i++)
      ans = ans + data.get(i).toString() + ", ";
    ans = ans + "]";
    return ans;
  }
}

class NullInputFormat extends
    FileInputFormat<Edge, BigList>{

  public RecordReader<Edge, BigList> createRecordReader(
      InputSplit input, TaskAttemptContext context)
      throws IOException, InterruptedException{
    return new NullRecordReader(context, input);
  }

}

class NullRecordReader extends RecordReader<Edge,BigList> {

  private LineRecordReader lineReader;
  private LongWritable lineKey;
  private Text lineValue;
  Edge key;
  BigList val;

  public NullRecordReader(TaskAttemptContext job, InputSplit split) throws IOException {
    lineReader = new LineRecordReader();
    lineReader.initialize(split, job);
  }

  public void initialize(InputSplit split, TaskAttemptContext context) {
    return;
  }

  public boolean nextKeyValue() throws IOException {

    if (!lineReader.nextKeyValue()) {
      return false;
    }

    key = new Edge();
    val = new BigList(2);

    return true;
  }

  public Edge getCurrentKey(){
    return key;
  }

  public BigList getCurrentValue(){
    return val;
  }

  public void close() throws IOException {
    lineReader.close();
  }

  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }
}

public class Tiling {
  int depth = 5;

  /* Get double the coordinates of the face */
  private static int[] face(int[] path1, int[] path2){
    int[] result = {0,0,0};
    for (int i = 0; i < path1.length -1; i++){
      if (path1[i] == path2[i]){
        result[path1[i]] += 2;
      } else if (path1[i] == path2[i+1] && path1[i+1] == path2[i]){
        result[path1[i]] += 1;
        result[path1[i+1]] += 1;
        result[1] *= -1;
        return result;
      } else {
        return result;
      } 
    }
    return result;
  }

  /* Partial ording on faces */
  public static boolean compare(int[] face1, int[] face2){
    if (face1[2] != face2[2]) return face1[2] > face2[2];
    else if (face1[0] != face2[0]) return face1[0] < face2[0];
    else if (face1[2]%2 == 1) return face1[1] > face2[1];
    else return face1[1] < face2[1]; 
  }
 
  public static class Map extends Mapper<Edge, BigList, Edge, BigList> {
 
    public void map(Edge inEdge, BigList inVals, Context output) throws IOException, InterruptedException {
      if (output.getConfiguration().get("mode").equals("bootstrap")){
        System.out.format("Bootstrapping %n");
        int a,b,c,d,n;
        a = b = c = 2; d = 2;
        n = a+b+c;

        int[] path0 = new int[n];
        int[] path1 = new int[n];
        int[] path2 = new int[n];
        for (int i = 0; i < n; i++){
          if (i < a) path0[i] = path1[i] = path2[i] = 0;
          else if (i < a+b) path0[i] = path1[i] = path2[i] = 1;
          else path0[i] = path1[i] = path2[i] = 2;
        }
        path1[a] = 0; path1[a-1] = 1;
        path2[a+b] = 1; path2[a+b-1] = 2;

        BigList seed1 = new BigList(d);
        BigList seed2 = new BigList(d);
        seed1.data.set(0,BigInteger.valueOf(1));
        seed2.data.set(0,BigInteger.valueOf(1));
        Edge edge1 = new Edge(path0, path1); 
        Edge edge2 = new Edge(path0, path2); 
        output.write(edge1, seed1);
        output.write(edge2, seed2);
//        System.out.format(edge1.toString() + " => " + seed1.toString() + "%n");
//        System.out.format(edge2.toString() + " => " + seed2.toString() + "%n");
        return;
      }

      int tmp;
      int[] path = inEdge.dst;
      int[] face_old = face(inEdge.src, inEdge.dst);
      int[] face_new = {0,0,0};

      /* loop over possible swaps */
      for (int i = 0; i < path.length-1; i++){
        if (path[i+1] > path[i]){ // if a swap...
          int[] newPath = new int[path.length];
          System.arraycopy(path, 0, newPath, 0, path.length);
          /* make the new path */
          tmp = newPath[i];
          newPath[i] = newPath[i+1];
          newPath[i+1] = tmp;

          /* compute the face */
          face_new = face(path, newPath);

          /* Slide the descents list down if needed */
          BigList newVals = new BigList(inVals);
          if (compare(face_old, face_new)){
            newVals.data.add(0, BigInteger.valueOf(0));
            newVals.data.remove(newVals.data.size()-1);
          }

          /* construct the new key */
          Edge newEdge = new Edge(path, newPath);

          /* Record it */
          output.write(newEdge, newVals);
        } // end if
      } // end for 
    } // end map
  } // end Map
 
  public static class Reduce extends Reducer<Edge, BigList, Edge, BigList> {
    public void reduce(Edge key, Iterable<BigList> values, Context output) throws IOException, InterruptedException {
      BigList vals = null;
      /* sum all the BigInts */
      for (BigList val : values){
//        System.out.format(key.toString() + " => " + val.toString() + "%n");
        if (vals == null)
          vals = new BigList(val);
        else
          for (int i = 0; i < val.data.size(); i++)
            vals.data.set(i,vals.data.get(i).add(val.data.get(i))); 
      }
      output.write(key, vals);
    } // end reduce
  } // end Reduce
 
  public static void main(String[] args) throws Exception {
    /* Parse args to see if this is a bootstrap run */
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    boolean bootstrap = false;
    boolean cont = false;
    boolean out = false; 
    if (otherArgs.length != 3) {
      System.err.println("Usage: tiling <mode> <indir> <outdir>");
      System.exit(2);
    }
   if (otherArgs[0].equals("bootstrap")) {
      bootstrap = true;
      conf.set("mode", "bootstrap"); 
      System.out.format("Bootstrapping %n");
    } else if (otherArgs[0].equals("continue")){
      cont = true;
      conf.set("mode", "continue");
    } else if (otherArgs[0].equals("output")){
      out = true;
      conf.set("mode", "output");
    }

    /* setup conf and job */
    Job job = new Job(conf, "tiling");

    /* Set the map, reduce, combine methods */
    job.setJarByClass(Tiling.class);
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    /* Set key and value types */
    //job.setMapOutputKeyClass(Edge.class);
    //job.setMapOutputValueClass(BigList.class);
    job.setOutputKeyClass(Edge.class);
    job.setOutputValueClass(BigList.class);

    /* Setup paths, dependent on bootstrap */
    if (bootstrap) {
      job.setInputFormatClass(NullInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
    } else if (cont) {
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
    } else if (out){
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    /* Get out */
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  } // end main

} // end class
