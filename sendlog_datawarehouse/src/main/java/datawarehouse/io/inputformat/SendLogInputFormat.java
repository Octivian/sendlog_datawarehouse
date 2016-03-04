package datawarehouse.io.inputformat;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

 
public class SendLogInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		reporter.setStatus(split.toString());
		return new SendLogRecordReader(job,(FileSplit)split);
	}
 
 
}
