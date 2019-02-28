
package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class StringSort {
   public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,"StringSort");
    job.setJarByClass(StringSort.class);

    //mapper 지정 (기본 Mapper 사용  , 입력되는 레코드가 그대로 출력 레코드가 됨 )
    job.setMapperClass(Mapper.class);        
    
    //Reducer 지정 (기본 Reducer 사용  , 맵에서 출력되는 것이  그대로  리듀스의 출력이 됨  )
    //리듀스 단에서 sort 됨
    job.setReducerClass(Reducer.class);

    //map 출력과  reduce 출력의 key, value type 지정
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //reduce의 수를 1로 설정
    job.setNumReduceTasks(1);
    
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    //입력파일
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //출력 디렉토리 (사이즈를 줄이기 위해 시퀀스 포맥 사용  60% 정도 감소 )
    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
    //Block 단위 압축 ()
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
   
    job.waitForCompletion(true);
    System.out.println("Job ended: " );
  }
}
/*
F:\myjar>hdfs dfs -ls
Found 1 items
drwxr-xr-x   - javains supergroup          0 2019-02-28 15:35 input

F:\myjar>hadoop jar hadoop_01.jar org.apache.hadoop.examples.StringSort input output

F:\myjar>hdfs dfs -ls
Found 2 items
drwxr-xr-x   - javains supergroup          0 2019-02-28 15:35 input
drwxr-xr-x   - javains supergroup          0 2019-02-28 15:37 output

F:\myjar>hdfs dfs -ls output
Found 2 items
-rw-r--r--   1 javains supergroup          0 2019-02-28 15:37 output/_SUCCESS
-rw-r--r--   1 javains supergroup        338 2019-02-28 15:37 output/part-r-00000

F:\myjar>hdfs dfs -text output/part-r-00000



*/