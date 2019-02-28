
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

    //mapper ���� (�⺻ Mapper ���  , �ԷµǴ� ���ڵ尡 �״�� ��� ���ڵ尡 �� )
    job.setMapperClass(Mapper.class);        
    
    //Reducer ���� (�⺻ Reducer ���  , �ʿ��� ��µǴ� ����  �״��  ���ེ�� ����� ��  )
    //���ེ �ܿ��� sort ��
    job.setReducerClass(Reducer.class);

    //map ��°�  reduce ����� key, value type ����
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //reduce�� ���� 1�� ����
    job.setNumReduceTasks(1);
    
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    //�Է�����
    FileInputFormat.addInputPath(job, new Path(args[0]));
    //��� ���丮 (����� ���̱� ���� ������ ���� ���  60% ���� ���� )
    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
    //Block ���� ���� ()
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