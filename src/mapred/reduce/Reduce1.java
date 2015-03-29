/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package mapred.reduce;


import java.io.IOException;
import java.util.ArrayList;
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
public class Reduce1 extends Reducer<Text, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Reduce1.class);
    SequenceFile.Writer writer;
    double support; //s
    IntWritable valueOut = new IntWritable();
    int totalMaps; //M
    int totalTransactions; //D
    ArrayList<String> blocksIds; //Partial
    
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        support = Double.parseDouble(context.getConfiguration().get("support"));
        String fileCachedPath = context.getConfiguration().get("fileCached");
        totalMaps = Integer.parseInt(context.getConfiguration().get("totalMaps"));
        totalTransactions = Integer.parseInt(context.getConfiguration().get("totalTransactions"));
        blocksIds = new ArrayList<String>();
        for(int i = 1; i <= totalMaps; i++){
    		blocksIds.add(context.getConfiguration().get("blockId"+i));
    	}
        
        Path path = new Path(fileCachedPath);
        log.info("Iniciando o REDUCE 1. Count Dir: "+count);
        log.info("Reduce1 support = "+support);
        
        log.info("Total Maps = "+totalMaps);
        System.out.println("\n*********-**********-************-*****************-**********");
        support = (support/100);
    }
    
    
    @Override
    /**
     * @param values suport:offsset:Di
     */
    public void reduce(Text key, Iterable<Text> values, Context context){
        int partialSupport = 0;
        int numMapsOfX = 0; //Nx
        String[] splitValues;
        Double partialGlobalSupport = new Double(0);
        ArrayList<String> diList = new ArrayList<String>();
        ArrayList<Integer> diSize = new ArrayList<Integer>();
        int di = 0;
        
    	for (Iterator<Text> it = values.iterator(); it.hasNext();) {
    		splitValues = it.next().toString().split(":");
            partialSupport += Integer.valueOf(splitValues[0]);
            diList.add(splitValues[1]);
            di += Integer.parseInt(splitValues[1]);
            diSize.add(Integer.valueOf(splitValues[2]));
            numMapsOfX++;
        }
    	di = di/numMapsOfX;
    	System.out.println("Itemset: "+key.toString());
    	System.out.println("Suporte parcial: "+partialSupport);
    	System.out.println("Suport threshold: "+support);
    	System.out.println("Número de Maps do item (Nx): "+numMapsOfX);
    	System.out.println("Valor de Di: "+di);
    	System.out.println("Toal de Maps: "+totalMaps);
    	System.out.println("Total de Transações: "+totalTransactions);
    	
    	if(numMapsOfX == totalMaps){
    		System.out.println("Item processado em todos os Maps, enviar para a partição global de itens frequentes");
    	}else{
	    	partialGlobalSupport = calcPartialGlobalSupport(di,numMapsOfX, partialSupport);
	    	System.out.println("Suporte Parcialmente Global: "+partialGlobalSupport);
	    	System.out.println("Mínimo suporte global: "+support*totalTransactions);
	        if(partialGlobalSupport >= (support*totalTransactions)){
	        	//Item parcialmente frequente, enviá-lo para partições em que não foi frequente]
	        	System.out.println("IEM parcialmente FREQUENTE---|");
	        	valueOut.set(partialGlobalSupport.intValue());
	            try {
	//            	saveInCache(key, valueOut);
	                context.write(key, valueOut);
	            } catch (IOException | InterruptedException ex) {
	                Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
	            }
	        }else{
	        	System.out.println("IEM NÃO FREQUENTE---|");
	        }
    	}
        System.out.println("\n*********-**********-************-*****************-**********");
    }
    
    public double calcPartialGlobalSupport(int di, int nx, int partialSuport){
    	return partialSuport + (((support * di)-1)*(totalMaps-nx));
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
//        try {
//			writer.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    }
}
