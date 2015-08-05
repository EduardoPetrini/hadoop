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
public class Reduce3 extends Reducer<Text, Text, Text, Text> {
    
    Log log = LogFactory.getLog(Reduce3.class);
    SequenceFile.Writer writer;
    String count;
    double support;
    int k;
    private Text valueOut;
    private IntWritable valueToCache;
    
    @Override
    public void setup(Context context) throws IOException{
        count = context.getConfiguration().get("count");
        support = Double.parseDouble(context.getConfiguration().get("support"));
        log.info("Iniciando o REDUCE 3. Count dir: "+count);
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        String fileCachedPath = context.getConfiguration().get("fileCachedWrited");
        
        writer = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(new Path(fileCachedPath)),
               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
        
        System.out.println("Support total: "+support);
        valueOut = new Text();
        valueToCache = new IntWritable();
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
        
    	int count = 1;
    	Iterator<Text> it = values.iterator();
    	int maxk = Integer.parseInt(it.next().toString().split(":")[0]);
    	
    	for (;it.hasNext();it.next()) {
            count ++;
        }
    	if(key.toString().trim().equals("1004 3191 8494")){
    		System.out.println("break here...");
    	}
    	if(count >= support){
	        try {
	        	/*Divide as saídas pelo k.*/
	        	valueOut.set(String.valueOf(count));
	        	valueToCache.set(count);
	        	if(key.toString().split(" ").length < maxk){
	        		context.write(key, valueOut);
	        	}else{
	        		save(key, valueToCache);
	        	}
	        } catch (IOException | InterruptedException ex) {
	            Logger.getLogger(Reduce3.class.getName()).log(Level.SEVERE, null, ex);
	        }
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
