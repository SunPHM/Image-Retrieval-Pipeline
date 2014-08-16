package ir.feature;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


public class ImageRecordReader extends RecordReader<Text, LongWritable> {
	

    /** The path to the file to read. */
    private final Path mFileToRead;
    /** The length of this file. */
    private final long mFileLength;

    /** The Configuration. */
   // private final org.apache.hadoop.conf.Configuration mConf;

    /** Whether this FileSplit has been processed. */
    private boolean mProcessed;
    /** Single Text to store the file name of the current file. */
  //  private final Text mFileName;
    /** Single Text to store the value of this file (the value) when it is read. */
    private final Text mFileText;
    private Text key;
    private LongWritable value;

    /**
     * Implementation detail: This constructor is built to be called via
     * reflection from within CombineFileRecordReader.
     *
     * @param fileSplit The CombineFileSplit that this will read from.
     * @param context The context for this task.
     * @param pathToProcess The path index from the CombineFileSplit to process in this record.
     */
    public ImageRecordReader(CombineFileSplit fileSplit, TaskAttemptContext context,
        Integer pathToProcess) {
      mProcessed = false;
      mFileToRead = fileSplit.getPath(pathToProcess);
      mFileLength = fileSplit.getLength(pathToProcess);
  //    mConf = context.getConfiguration();

      assert 0 == fileSplit.getOffset(pathToProcess);

  //    mFileName = new Text();
      mFileText = new Text();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mFileText.clear();
    }

    /**
     * Returns the absolute path to the current file.
     *
     * @return The absolute path to the current file.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return this.key;
    }

    /**
     * <p>Returns the current value.  If the file has been read with a call to NextKeyValue(),
     * this returns the contents of the file as a BytesWritable.  Otherwise, it returns an
     * empty BytesWritable.</p>
     *
     * <p>Throws an IllegalStateException if initialize() is not called first.</p>
     *
     * @return A BytesWritable containing the contents of the file to read.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
      return this.value;
    }

    /**
     * Returns whether the file has been processed or not.  Since only one record
     * will be generated for a file, progress will be 0.0 if it has not been processed,
     * and 1.0 if it has.
     *
     * @return 0.0 if the file has not been processed.  1.0 if it has.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
      return (mProcessed) ? (float) 1.0 : (float) 0.0;
    }

    /**
     * All of the internal state is already set on instantiation.  This is a no-op.
     *
     * @param split The InputSplit to read.  Unused.
     * @param context The context for this task.  Unused.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      // no-op.
    }

    /**
     * <p>If the file has not already been read, this reads it into memory, so that a call
     * to getCurrentValue() will return the entire contents of this file as Text,
     * and getCurrentKey() will return the qualified path to this file as Text.  Then, returns
     * true.  If it has already been read, then returns false without updating any internal state.</p>
     *
     * @return Whether the file was read or not.
     * @throws IOException if there is an error reading the file.
     * @throws InterruptedException if there is an error.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!mProcessed) {
        if (mFileLength > (long) Integer.MAX_VALUE) {
          throw new IOException("File is longer than Integer.MAX_VALUE.");
        }
        this.key=new Text(mFileToRead.getName());
        this.value=new LongWritable(mFileLength);
        	
        mProcessed = true;
        return true;
      }
      return false;
    }


}