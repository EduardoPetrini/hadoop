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
 * Salva o conjunto de 2itemsets e seus respectivos tids.
 * @author eduardo
 */
public class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    Log log = LogFactory.getLog(Reduce2.class);
    SequenceFile.Writer writer;
    double support;
    IntWritable valueOut = new IntWritable();
    
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileCachedPath = context.getConfiguration().get("fileCachedWrited")+"-"+System.currentTimeMillis();
        support = Double.parseDouble(context.getConfiguration().get("support"));
        
        Path path = new Path(fileCachedPath);
        log.info("AprioriDpc REDUCE 2");
        
         writer = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(path),
               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
         System.out.println("Support: "+support);
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
            	saveInCache(key, valueOut);
                context.write(key, valueOut);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
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
        log.info("AprioriDpc Finalizando o REDUCE 2.");
        
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(Reduce2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
