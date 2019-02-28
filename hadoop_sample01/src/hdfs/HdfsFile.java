package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFile {

	public static void main(String[] args) {
		if(args.length != 2) {
			System.out.println("실행 방법 : HdfsFile <filename> <contents>");
			System.exit(2);
		}
		
		Configuration conf = null;
		FileSystem hdfs = null;
		Path path = null;
		FSDataOutputStream os = null;
		FSDataInputStream is = null;
	    String inputString = null;
        try {
           conf = new Configuration();
           hdfs = FileSystem.get(conf);
           path = new Path(args[0]);
           if(hdfs.exists(path)) {
        	   hdfs.delete(path,true);
           }
           
           os = hdfs.create(path);
           os.writeUTF(args[1]);
           os.close();
           
           is = hdfs.open(path);
           inputString = is.readUTF();
           is.close();
        	
        }catch (Exception e) {
        	e.printStackTrace();
		}finally {
			try {
				if(os != null) os.close();
				if(is != null) is.close();
				if(hdfs != null) hdfs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        System.out.println("***** HDFS  READ DATA *****");
        System.out.println(inputString);

        System.out.println("HdfsFile END");
	}

}
