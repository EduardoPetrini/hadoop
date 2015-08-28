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
    
	private Log log = LogFactory.getLog(Reduce2.class);
	private SequenceFile.Writer writer;
	private double support;
	private IntWritable valueOut = new IntWritable();
    
    @Override
    public void setup(Context context) throws IOException{
        String strK = context.getConfiguration().get("k");
        support = Double.parseDouble(context.getConfiguration().get("support"));
        
        log.info("AprioriCpa o Reduce para contar e podar C"+strK);
        
//         writer = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(path),
//               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
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
//            	saveInCache(key, valueOut);
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
        log.info("AprioriCpa Finalizando o REDUCE contagem.");
        
//        try {
//            writer.close();
//        } catch (IOException ex) {
//            Logger.getLogger(Reduce2.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
    
    
}
