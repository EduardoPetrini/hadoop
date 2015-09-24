/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;

/**
 * Gerar itemsets de tamanho 2.
 * @author eduardo
 */
public class Map2  extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    private Log log = LogFactory.getLog(Map2.class);
    private SequenceFile.Reader reader;
    private HashPrefixTree hpt;
    private int k;
    private Text keyOut;
    private IntWritable valueOut;
    
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
    	int candSize = Integer.parseInt(context.getConfiguration().get("candsize"));
    	String[] filesName = new String[candSize];
    	
    	log.info("AprioriCpa Map contagem de C"+k);
    	log.info("Arquivo de entrada no inputCandidates: ");
    	hpt = new HashPrefixTree();
    	Path path;
    	for(int i = 0; i < candSize; i++){
    		filesName[i] = context.getConfiguration().get("inputCandidates"+i);
    		log.info(filesName[i]);
    		path = new Path(filesName[i]);
    		reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
    		
    		openFile(context);
    	}
    	
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        
        
        
        keyOut = new Text();
        valueOut = new IntWritable(1);
    }
    
    /**
     * 
     * @param transaction
     * @param hNode
     * @param i
     * @param k
     * @param itemset
     * @param itemsetIndex
     * @param context
     */
    private void subSet(String[] transaction, HashNode hNode, int i,
			String[] itemset, int itemsetIndex, Context context) {
    	if(i >= transaction.length){
			return;
		}
		
		HashNode son = hNode.getHashNode().get(transaction[i]);
		
		if(son == null){
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			
			if(hNode.getLevel() == k-1){
				StringBuilder sb = new StringBuilder();
				for(String item: itemset){
					if(item != null){
						sb.append(item).append(" ");
					}
				}
				// System.out.println("Encontrou: "+sb.toString().trim());
				keyOut.set(sb.toString().trim());
				try {
					context.write(keyOut, valueOut);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				itemset[itemsetIndex] = "";
				return;
			}
			
			i++;
			itemsetIndex++;
			while(i < transaction.length){
				subSet(transaction, son, i, itemset, itemsetIndex, context);
				for(int j = itemsetIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
	}
    
    @Override
    public void map(LongWritable key, Text value, Context context){
    	String[] transaction;
    	String[] itemset;
		boolean endBlock = false;
		int pos;
		int start = 0;
		int len;

		while ((pos = value.find("\n", start)) != -1) {
			len = pos - start;
			try {
				transaction = Text.decode(value.getBytes(), start, len).trim().split(" ");
				for(int i = 0; i < transaction.length; i++){
		    		itemset = new String[k];
		    		subSet(transaction, hpt.getHashNode(), i, itemset, 0,context);
		    	}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
			start = pos + 1;
			if (start >= value.getLength()) {
				endBlock = true;
				break;
			}
		}
		// pegar a ultima transação, caso tenha
		if (!endBlock) {
			len = value.getLength() - start;
			try {
				transaction = Text.decode(value.getBytes(), start, len).split(" ");
				for(int i = 0; i < transaction.length; i++){
		    		itemset = new String[k];
		    		subSet(transaction, hpt.getHashNode(), i, itemset, 0,context);
		    	}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
    }
    
    /**
     * 
     * @param path
     * @param context
     * @return
     */
    public void openFile(Context context){
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				hpt.add(hpt.getHashNode(),key.toString().split(" "),0);
	        }
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
    }
}
