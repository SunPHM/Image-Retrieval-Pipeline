package ir.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TextInputFormat;

public class NonSplitTextInputFormat extends TextInputFormat {

    protected boolean isSplitable(JobContext context, Path file) {
            // TODO Auto-generated method stub
            return false;
    }

}
