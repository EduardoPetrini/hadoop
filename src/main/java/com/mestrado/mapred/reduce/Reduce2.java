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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Salva o conjunto de 2itemsets e seus respectivos tids.
 * @author eduardo
 */
public class Reduce2 extends Reducer<Text, Text, Text, Text> {
    
    Log log = LogFactory.getLog(Reduce2.class);
    double support;
    Text valueOut = new Text();
    
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        support = Double.parseDouble(context.getConfiguration().get("support"));//Definido no initial config
        log.info("Iniciando o REDUCE 2. Count dir: "+count);
        System.out.println("Support global: "+support);
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
    	String[] firstValue = values.iterator().next().toString().split("#");
    	int count = Integer.parseInt(firstValue[0]); //suporte da fase 1
    	count += Integer.parseInt(firstValue[1]);//suporte count da fase 2
    	
    	for (Iterator<Text> it = values.iterator(); it.hasNext();) {
            count += Integer.parseInt(it.next().toString().split("#")[1]);
        }

    	if(count >= support){
        	valueOut.set(String.valueOf(count));
            try {
                context.write(key, valueOut);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 2.");
        
    }
}
