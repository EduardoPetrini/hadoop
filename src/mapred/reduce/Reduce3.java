/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.reduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author eduardo
 */
public class Reduce3 extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    Log log = LogFactory.getLog(Reduce3.class);
    SequenceFile.Writer writer;
    String count;
    int k;
    
    @Override
    public void setup(Context context) throws IOException{
        count = context.getConfiguration().get("count");
        log.info("Iniciando o REDUCE 3. Count dir: "+count);
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        String fileCachedPath = context.getConfiguration().get("fileCachedWrited");
        
        writer = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(new Path(fileCachedPath)),
               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
    }
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        
    	int count = 0;
    	for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) {
            count += it.next().get();
        }
    	
        try {
        	/*Divide as saídas pelo k. k e k+1 para a saída default*/
        	if(key.toString().split(" ").length < (k+2)){
        		context.write(key, new IntWritable(count));
        	}else{
        		save(key, new IntWritable(count));
        	}
        } catch (IOException | InterruptedException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * 
     * @param key
     * @param value 
     */
    public void save(Text key, IntWritable value){
        try {
            writer.append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 3.");
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
