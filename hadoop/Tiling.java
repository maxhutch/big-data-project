package org.myorg;
 
/* Java includes */ 
import java.io.IOException;
import java.util.*;
import java.math.BigInteger;

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

public class Tiling {
  int depth = 5;

  /* Get double the coordinates of the face */
  private static int[] face(int[] path1, int[] path2){
    int[] result = new int[3];
    for (int i = 0; i < path1.length -1; i++){
      if (path1[i] == path2[i]){
        result[path1[i]] += 2;
      } else if (path1[i] == path2[i+1] && path1[i+1] == path2[i]){
        result[path1[i]] += 1;
        result[path1[i+1]] += 1;
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
    else if (face1[1] != face2[1]) return face1[1] < face2[1];
    else if (face1[0]%2 == 0) return face1[0] > face2[0];
    else return face1[0] < face2[0]; 
  }
 
  public static class Map extends Mapper<int[][], List<BigInteger>, int[][], List<BigInteger>> {
 
    public void map(int[][] inEdge, List<BigInteger> inVals, Context output) throws IOException, InterruptedException {
      int tmp;
      int[] path = inEdge[1];
      int[] face_old = face(inEdge[0], inEdge[1]);
      int[] face_new = {0,0,0};

      /* loop over possible swaps */
      for (int i = 0; i < path.length-1; i++){
        if (path[i+1] < path[i]){ // if a swap...
          int[] newPath = new int[path.length];
          System.arraycopy(path, 0, newPath, 0, path.length);
          /* make the new path */
          tmp = newPath[i];
          newPath[i] = newPath[i+1];
          newPath[i+1] = tmp;

          /* compute the face */
          face_new = face(path, newPath);

          /* Slide the descents list down if needed */
          ArrayList<BigInteger> newVals = new ArrayList<BigInteger>(inVals);
          if (compare(face_new, face_old)){
            newVals.add(0, BigInteger.valueOf(0));
            newVals.remove(newVals.size());
          }

          /* construct the new key */
          int[][] newEdge = new int[2][path.length];
          newEdge[0] = path;
          newEdge[1] = newPath;

          /* Record it */
          output.write(newEdge, newVals);
        } // end if
      } // end for 
    } // end map
  } // end Map
 
  public static class Reduce extends Reducer<int[][], List<BigInteger>, int[][], List<BigInteger>> {
    public void reduce(int[][] key, Iterable<List<BigInteger>> values, Context output) throws IOException, InterruptedException {
      ArrayList<BigInteger> vals = new ArrayList<BigInteger>();
      /* sum all the BigInts */
      for (List<BigInteger> val : values) {
        for (int i = 0; i < val.size(); i++)
          vals.set(i,vals.get(i).add(val.get(i))); 
      }
      output.write(key, vals);
    } // end reduce
  } // end Reduce
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: tiling <indir> <outdir>");
      System.exit(2);
    }
    Job job = new Job(conf, "tiling");
    job.setJarByClass(Tiling.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(int[][].class);
    job.setOutputValueClass(ArrayList.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  } // end main

} // end class
