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
    long totalTransaction;
    
    @Override
    public void setup(Context context) throws IOException{
    	String sup = context.getConfiguration().get("supportPercentage");
    	support = Double.parseDouble(sup);
    	totalTransaction= Long.parseLong(context.getConfiguration().get("totalTransactions"));
        log.info("Iniciando o REDUCE ImrApriori Fase 2. ");
        System.out.println("Support: "+support);
    }
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
    	Iterator<Text> it = values.iterator();
    	String[] valueItem = it.next().toString().split(":");
    	int count = Integer.parseInt(valueItem[0]);
    	count += Integer.parseInt(valueItem[1]);
    	
    	while (it.hasNext()) {
            count += Integer.parseInt(it.next().toString().split(":")[1]);
        }
    	
    	/*String[] firstValue = values.iterator().next().toString().split("#");
    	int count = Integer.parseInt(firstValue[0]); //suporte da fase 1
    	count += Integer.parseInt(firstValue[1]);//suporte count da fase 2
    	
    	for (Iterator<Text> it = values.iterator(); it.hasNext();) {
            count += Integer.parseInt(it.next().toString().split("#")[1]);
        }*/
    	
    	if(((count/((double) totalTransaction)) >= support)){
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
