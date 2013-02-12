package mx.itam.metodos.common;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;

public final class FSHelper {
  
   private static Logger LOGGER = Logger.getLogger(FSHelper.class.getName()); 

  public static Path[] getPaths(Path path, Configuration conf) throws IOException {   
    FileSystem fs = path.getFileSystem(conf);
    return FileUtil.stat2Paths(fs.listStatus(path, new OutputFilesFilter()));
  }

  public static SequenceFile.Reader getReader(Path path, Configuration conf) throws IOException {
    LOGGER.info("getReader: " + path.toString());
    FileSystem fs = path.getFileSystem(conf);
    return new SequenceFile.Reader(fs, path, conf);
  }

  public static FSDataOutputStream create(Path path, Configuration conf) throws IOException {
    LOGGER.info("create: " + path.toString());
    FileSystem fs = path.getFileSystem(conf);
    return fs.create(path);
  }
  
  public static FSDataInputStream open(Path path, Configuration conf) throws IOException {
    LOGGER.info("create: " + path.toString());
    FileSystem fs = path.getFileSystem(conf);
    return fs.open(path);
  }

  public static String getQualified(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    LOGGER.info(String.format("getQualified: %s->%s\n", path, path.makeQualified(fs)));
    return path.makeQualified(fs).toString();
  }
}