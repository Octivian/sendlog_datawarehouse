package datawarehouse.io.inputformat;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import datawarehouse.io.serde.SendLogSerde;

public class SendLogRecordReader implements RecordReader<LongWritable, Text> {

	// Reader
    private LineRecordReader reader;
    // The current line_num and line
    private LongWritable lineKey = null;
    private Text lineValue = null;
    private static final String LINE_SEP = "Src: ";
    // Each log  stored in
    private StringBuilder sb = new StringBuilder();
    
 
    public SendLogRecordReader(JobConf job, FileSplit split) throws IOException {
        reader = new LineRecordReader(job, split);
        lineKey = reader.createKey();
        lineValue = reader.createValue();
    }
 
    public synchronized void close() throws IOException {
        reader.close();
    }
 
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        while (true) {
            if (!reader.next(lineKey, lineValue)) {
            	// 处理文件末尾结余的stringbuilder
            	if(sb.length()!=0){
            		key.set(lineKey.get());
                    value.set(sb.toString());
                    sb.delete(0, sb.length());
                    return true;
            	}else{
            		break;
            	}
            }
            
            
            String line = lineValue.toString();
            if (StringUtils.isEmpty(line)){
            	continue;
            } else if(line.startsWith(LINE_SEP)){
            	if(sb.length()!=0){
            		key.set(lineKey.get());
                    value.set(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(line);
                    return true;
            	}else{
            		sb.append(line);
            	}
            }else{
            	sb.append(SendLogSerde.COLUMN_SEP);
            	sb.append(line);
            }
        }
        return false;
    }
    
 
    public float getProgress() throws IOException {
        return reader.getProgress();
    }
 
    public LongWritable createKey() {
        return new LongWritable();
    }
 
    public Text createValue() {
        return new Text();
    }
 
    public synchronized long getPos() throws IOException {
        return reader.getPos();
    }
}
