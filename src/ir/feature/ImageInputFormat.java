package ir.feature;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


//only reads the file name and the files size. size is optional
public class ImageInputFormat extends CombineFileInputFormat<Text, LongWritable> {

	@Override
	 public RecordReader<Text, LongWritable> createRecordReader(
	            InputSplit split, TaskAttemptContext context) throws IOException {

	        if (!(split instanceof CombineFileSplit)) {
	              throw new IllegalArgumentException("split must be a CombineFileSplit");
	            }
	            return new CombineFileRecordReader<Text, LongWritable>((CombineFileSplit) split, context, ImageRecordReader.class);
	    }

	  @Override
      protected boolean isSplitable(JobContext context, Path file) {
          return false;
      }
	
}