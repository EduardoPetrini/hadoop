/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;

import main.java.com.mestrado.app.PrefixTree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Gerar itemsets de tamanho k, k+1 e k+3 em uma única instância Map/Reduce.
 * @author eduardo
 */
public class Map3  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    Log log = LogFactory.getLog(Map3.class);
    IntWritable countOut = new IntWritable(1);
    SequenceFile.Reader reader;
    PrefixTree prefixTree;
    int mink, maxk;
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        String fileSequenceInput = context.getConfiguration().get("fileSequenceInput");
        maxk = Integer.parseInt(context.getConfiguration().get("maxk"));
        mink = Integer.parseInt(context.getConfiguration().get("mink"));
        
        log.info("Iniciando map 3 count = "+count);
        log.info("Arquivo Cached = "+fileSequenceInput);
        
        Path path = new Path(fileSequenceInput);
        
        reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        openFile(fileSequenceInput, context);
        
        System.out.println("MinK "+mink+" maxK "+maxk);
    }
    
    /**
     * 
     * @param transaction
     * @param pt
     * @param i
     * @param itemset
     * @param itemsetIndex
     * @param context
     */
    private void subSet(String[] transaction, PrefixTree pt, int i, String[] itemset, int itemsetIndex, Context context) {
    	
    	if(i >= transaction.length){
			return;
		}

		if(pt.getLevel() > maxk){
			return;
		}

		int index =  pt.getPrefix().indexOf(transaction[i]);
		
		if(index == -1){
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			i++;
			if(pt.getLevel() >= mink-1){
				StringBuilder sb = new StringBuilder();

				for(String s: itemset){
					if(s != null && !s.isEmpty())
						sb.append(s).append(" ");
				}
				
				//envia para o reduce
				try{
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}catch(IOException | InterruptedException e){
					e.printStackTrace();
					System.exit(1);
				}
				
//				itemset[itemsetIndex] = "";
//				return;
			}
			
			if(pt.getPrefixTree().isEmpty() || pt.getPrefixTree().size() <= index || pt.getPrefixTree().get(index) == null){
				itemset[itemsetIndex] = "";
				return;
			}else{
				itemsetIndex++;
				while(i < transaction.length){
					subSet(transaction, pt.getPrefixTree().get(index),i, itemset, itemsetIndex, context);
					i++;
				}
			}
		}
	}
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	
		//Aplica a função subset e envia o itemset para o reduce
    	String[] transaction = value.toString().split(" ");
    	String[] itemset = new String[maxk];

    	if(transaction.length >= mink){

    		for(int i = 0; i < transaction.length; i++){
    			subSet(transaction, prefixTree, i, itemset, 0, context);
    		}
    	}
    }
    
    /**
     * 
     * @param path
     * @param context
     * @return
     */
    public void openFile(String path, Context context){
    	try {
			prefixTree =  new PrefixTree(0);
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				//System.out.println("Add Key: "+key.toString());
//	            fileCached.add(key.toString());
				prefixTree.add(prefixTree, key.toString().split(" "), 0);
	        }
//			long begin = System.currentTimeMillis();
//			Collections.sort(fileCached, NUMERIC_ORDER);
//			long end = System.currentTimeMillis();
//			double total = end-begin;
//			if((total/1000) > 1){
//				System.out.println("Tempo gasto para ordenação: "+total+" milis ou "+total/1000+" segundos...");
//			}
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
