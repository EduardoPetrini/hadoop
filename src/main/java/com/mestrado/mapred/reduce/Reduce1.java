/*

 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.reduce;


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

/**
 *
 * @author eduardo
 */
public class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Reduce1.class);
    double support;
    IntWritable valueOut = new IntWritable();
    SequenceFile.Writer writer;
    
    
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        support = Double.parseDouble(context.getConfiguration().get("support"));
        log.info("Iniciando o REDUCE 1. Count Dir: "+count);
        log.info("Reduce1 support = "+support);
        
        String fileSequenceOutput = context.getConfiguration().get("fileSequenceOutput");
        Path path = new Path(fileSequenceOutput);
        
        writer = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
    }
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context){
        int count = 0;
    	for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) {
            count += it.next().get();
        }
    	
        if(count >= support){
        	valueOut.set(count);
        	try {
				context.write(key, valueOut);
				saveInCache(key, valueOut);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
       
    public void saveInCache(Text key, IntWritable value){
    	try {
            writer.append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 1.");
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
