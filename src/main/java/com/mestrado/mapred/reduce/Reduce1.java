/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.reduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import main.java.com.mestrado.utils.MrUtils;

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
    SequenceFile.Writer[] writers;
    double support; //s
    IntWritable valueOut = new IntWritable();
    int totalMaps; //M
    int totalTransactions; //D
    ArrayList<String> blocksIds; //Partial
    
    @Override
    /**
     * Antes de inicializar o Reduce
     */
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        support = Double.parseDouble(context.getConfiguration().get("supportPercentage"));//Definido no Main
        String writersFileName = context.getConfiguration().get("outputPartialName");
        totalMaps = Integer.parseInt(context.getConfiguration().get("totalMaps"));
        totalTransactions = Integer.parseInt(context.getConfiguration().get("totalTransactions"));
        blocksIds = new ArrayList<String>();
        writers = new SequenceFile.Writer[totalMaps];
        String partitionFileName;
        
        for(int i = 1; i <= totalMaps; i++){
        	partitionFileName = writersFileName+context.getConfiguration().get("blockId"+i);
			blocksIds.add(context.getConfiguration().get("blockId"+i));
    		writers[i-1] = SequenceFile.createWriter(context.getConfiguration(), SequenceFile.Writer.file(new Path(partitionFileName)),
    	               SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(IntWritable.class));
    	}
        
        log.info("Iniciando o REDUCE 1. Count Dir: "+count);
        log.info("Reduce1 support = "+support);
        
        log.info("Total Maps = "+totalMaps);
        System.out.println("\n*********-**********-************-*****************-**********");
        
    }
    
    /**
     * 
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context){
        int partialSupport = 0;
        int numMapsOfX = 0; //Nx
        String[] splitValues;
        Double partialGlobalSupport = new Double(0);
        ArrayList<String> diList = new ArrayList<String>();
//        ArrayList<Integer> diSize = new ArrayList<Integer>();
        int di = 0;
        String rm;
        log.info("Obtém a lista de valores para a chave "+key);
    	for (Iterator<Text> it = values.iterator(); it.hasNext();) {
    		rm = it.next().toString();
    		splitValues = rm.split(":");
    		log.info(rm);
            partialSupport += Integer.valueOf(splitValues[0]);
            diList.add(splitValues[1]);
            di += Integer.parseInt(splitValues[2]);
//            diSize.add(Integer.valueOf(splitValues[2]));
            numMapsOfX++;
        }
    	
    	di = di/numMapsOfX;
    	valueOut.set(partialSupport);
    	
    	if(numMapsOfX >= totalMaps){
    		if(partialSupport > (support*totalTransactions)){
//	    		System.out.println("Item processado em todos os Maps, enviar para a partição global de itens frequentes");
	    		try {
	                context.write(key, valueOut);
	            } catch (IOException | InterruptedException ex) {
	                Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
	            }
    		}
    	}else{
	    	partialGlobalSupport = calcPartialGlobalSupport(di,numMapsOfX, partialSupport);
//	    	System.out.println("Suporte Parcialmente Global: "+partialGlobalSupport);
	        if(partialGlobalSupport >= (support*totalTransactions)){
	        
	        	//Item parcialmente frequente, enviá-lo para partições em que não foi frequente]
//	        	System.out.println("IEM parcialmente FREQUENTE---|");
	        	
	        	boolean cameFromThePartition;
	        	
	        	for_ext:
	        	for(int i = 0; i < blocksIds.size(); i++){
	        		cameFromThePartition = false;
	        		for(int j = 0; j < diList.size(); j++){
	        			if(MrUtils.checkPartitions(blocksIds.get(i),diList.get(j),100)){
	        				cameFromThePartition = true;
//	        				System.out.println("Veio da partição "+diList.get(j));
	        				continue for_ext;
	        			}
	        		}
	        		if(!cameFromThePartition){
//	        			System.out.println("Vai para partição "+blocksIds.get(i));
	        			saveInCache(key, valueOut, i);
	        		}
	        	}
	        
	        }else{
//	        	System.out.println("IEM NÃO FREQUENTE---|");
	        }
    	}
//        System.out.println("\n*********-**********-************-*****************-**********");
    }
    
    public void printInfo(String key, double partialSupport, int numMapsOfX, int di){
    	System.out.println("Itemset: "+key);
    	System.out.println("Suporte parcial: "+partialSupport);
    	System.out.println("Suport threshold: "+support);
    	System.out.println("Número de Maps do item (Nx): "+numMapsOfX);
    	System.out.println("Valor de Di: "+di);
    	System.out.println("Toal de Maps: "+totalMaps);
    	System.out.println("Total de Transações: "+totalTransactions);
    	System.out.println("Mínimo suporte global: "+support*totalTransactions);
    }
    
    
    /**
     * 
     * @param di
     * @param nx
     * @param partialSuport
     * @return
     */
    public double calcPartialGlobalSupport(int di, int nx, int partialSuport){
    	return partialSuport + (((support * di)-1)*(totalMaps-nx));
    }
       
    /***
     * 
     * @param key
     * @param value
     * @param index
     */
    public void saveInCache(Text key, IntWritable value, int index){
    	try {
            writers[index].append(key, value);
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Ao finalizar o Reduce
     */
    @Override
    public void cleanup(Context c){
        log.info("Finalizando o REDUCE 1.");
        try {
        	for(SequenceFile.Writer writer: writers){
        		writer.close();
        	}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}