package hadoop.inputformat;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

public class WholeSplitRecordReader extends RecordReader<LongWritable, Text>{
	
	private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private static final Log LOG = LogFactory.getLog(
            WholeSplitRecordReader.class);

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (in != null) {
            in.close();
        }
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 if (start == end) {
	            return 0.0f;
	        } else {
	            return Math.min(1.0f, (pos - start) / (float) (end - start));
	        }
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		// This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) genericSplit;
        
        // Retrieve configuration, and Max allowed
        // bytes for a single record
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(
                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
 
        // Split "S" is responsible for all records
        // starting from "start" and "end" positions
        start = split.getStart();
        end = start + split.getLength();
 
        // Retrieve file containing Split "S"
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
 
        // If Split "S" starts at byte 0, first line will be processed
        // If Split "S" does not start at byte 0, first line has been already
        // processed by "S-1" and therefore needs to be silently ignored
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            // This is to make sure we won't miss any line
            // It could happen if "start" is located on a EOL
            --start;
            fileIn.seek(start);
        }
 
        in = new LineReader(fileIn, job);
 
        // If first line needs to be skipped, read first line
        // and stores its content to a dummy Text
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
            start += in.readLine(dummy, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE, 
                            end - start));
        }
 
        // Position is the actual start
        this.pos = start;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;

        Text v = new Text();
        
        /*Ler toda a entrada de uma vez*/
        LOG.info("Lendo toda a entrada do map de uma vez... size: "+end);
        while (pos < end) {
            newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
            value.append(v.getBytes(),0, v.getLength());
            value.append(endline.getBytes(),0, endline.getLength());
            if (newSize == 0) {
                break;
            }
            pos += newSize;
        }
        
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
	}

}
