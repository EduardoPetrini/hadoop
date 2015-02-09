package hadoop.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class WholeSplitInputFormat extends FileInputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit,
			JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		return (RecordReader<LongWritable, Text>) new WholeSplitRecordReader();
	}
}
