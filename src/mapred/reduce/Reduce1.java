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

/**
 *
 * @author eduardo
 */
public class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Reduce1.class);
    double support;
    IntWritable valueOut = new IntWritable();
    
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        support = Double.parseDouble(context.getConfiguration().get("support"));
        log.info("Iniciando o REDUCE 1. Count Dir: "+count);
        log.info("Reduce1 support = "+support);
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
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
       
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 1.");
    }
}
