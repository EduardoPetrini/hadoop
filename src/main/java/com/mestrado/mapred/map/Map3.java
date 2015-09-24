/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;

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
public class Map3  extends Mapper<LongWritable, Text, Text, Text>{
    
    Log log = LogFactory.getLog(Map3.class);
    SequenceFile.Reader reader;
    ArrayList<String> fileCached;
    ArrayList<String> itemsetAux;
    HashPrefixTree hpt;
    int k;
    int mink, maxk = 0;
    private Text keyOut;
    private Text valueOut;
    /**
     * Le o arquivo invertido para a memória.
     * @param context
     * @throws IOException 
     */
    @Override
    public void setup(Context context) throws IOException{
        String count = context.getConfiguration().get("count");
        log.info("AprioriDpc Map 3 count = "+count);
        int lksize = Integer.parseInt(context.getConfiguration().get("lksize"));
        String[] fileCachedRead = new String[lksize];
        Path path;
        log.info("Arquivos Cached: ");
        fileCached = new ArrayList<String>();
        for(int i = 0; i < lksize; i++){
        	fileCachedRead[i] = context.getConfiguration().get("fileCachedRead"+i);
            log.info(fileCachedRead[i]);
        	path = new Path(fileCachedRead[i]);
        	reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(path));
        	openFile(fileCachedRead[i], context);
        }
        String kStr = context.getConfiguration().get("k");
        k = Integer.parseInt(kStr);
        double earlierTime = Double.parseDouble(context.getConfiguration().get("earlierTime"));
        
        log.info("Tempo da fase anterior é "+earlierTime);
        
        //Gerar combinações dos itens de acordo com o tamanho de lk e do tempo gasto da fase anterior
        
        hpt = new HashPrefixTree();
        itemsetAux = new ArrayList<String>();
        
        log.info("K is "+k);
        mink = k;
        
        String itemsetC;
        String[] itemsetCSpt;
//        prefixTree.printStrArray(fileCached);
        
        if(fileCachedRead != null && fileCached.size() > 0){
        	if(fileCached.get(fileCached.size()-1).split(" ").length < k-1){
	        	log.info("Itemsets é menor do que k");
	        	System.out.println("Saíndo da aplicação!");
	        	System.exit(0);
        	}
        }else{
        	log.info("Arquivo do cache distribuído é vazio!");
        	return;
        }
        
        int lkSize = fileCached.size();
        
        int ct;
        
        if(earlierTime >= 60){
        	ct = lkSize * 1;
        }else{
        	ct = (int)Math.round(lkSize * 1.2);
        }
        String[] itemA;
        String[] itemB;
        
        log.info("O valor de ct é "+ct);
        
        for (int i = 0; i < fileCached.size(); i++){
        	itemA = fileCached.get(i).split(" ");
        	for (int j = i+1; j < fileCached.size(); j++){
        		itemB = fileCached.get(j).split(" ");
        		if(isSamePrefix(itemA, itemB, i, j)){
        			itemsetCSpt = new String[k];
        			itemsetC = combine(itemA, itemB, itemsetCSpt);
        			if(allSubsetIsFrequent(itemsetCSpt, fileCached)){
	        			itemsetAux.add(itemsetC);
	//        			System.out.println(itemsetC+" no primeiro passo");
	        			//Building HashTree
	        			hpt.add(hpt.getHashNode(), itemsetC.split(" "), 0);
        			}
        			
        		}
        	}
        }
        
        log.info("Inicia o loop dinâmico");
        
        int cSetSize = itemsetAux.size();
        while( cSetSize <= ct){
        	//System.out.println("Cset size "+cSetSize);
        	fileCached.clear();
        	if(itemsetAux.isEmpty()){
        		System.out.println("Opa, break com k = "+k+" reduzido para k = "+(k-1));
        		k--;
        		break;
        	}
        	
        	k++;
	        for (int i = 0; i < itemsetAux.size(); i++){
	        	itemA = itemsetAux.get(i).split(" ");
	        	for (int j = i+1; j < itemsetAux.size(); j++){
	        		itemB = itemsetAux.get(j).split(" ");
	        		if(isSamePrefix(itemA, itemB, i, j)){
	        			itemsetCSpt = new String[k];
	        			itemsetC = combine(itemA, itemB, itemsetCSpt);
	        			
	        			if(allSubsetIsFrequent(itemsetCSpt, itemsetAux)){
	        				fileCached.add(itemsetC);
	        				hpt.add(hpt.getHashNode(), itemsetC.split(" "), 0);
	        			}
//	        			System.out.println(itemsetC+" no primeiro passo");
	        		}
	        	}
	        }
	        if(fileCached.isEmpty()){
	        	System.out.println("Opa, break com k = "+k+" reduzido para k = "+(k-1));
	        	k--;
        		break;
        	}
	        k++;
	        itemsetAux.clear();
	        for (int i = 0; i < fileCached.size(); i++){
	        	itemA = fileCached.get(i).split(" ");
	        	for (int j = i+1; j < fileCached.size(); j++){
	        		itemB = fileCached.get(j).split(" ");
	        		if(isSamePrefix(itemA, itemB, i, j)){
	        			itemsetCSpt = new String[k];
	        			itemsetC = combine(itemA, itemB, itemsetCSpt);
	        			if(allSubsetIsFrequent(itemsetCSpt, fileCached)){
	        				itemsetAux.add(itemsetC);
	        				hpt.add(hpt.getHashNode(), itemsetC.split(" "), 0);
	        			}
//	        			System.out.println(itemsetC+" no segundo passo");
	        		}
	        	}
	        }
	        cSetSize += itemsetAux.size();
	        System.out.println("No loop dinamico, cSetSize: "+cSetSize+", ct: "+ct);
        }
       
        maxk = k;
        System.out.println("MinK "+mink+" maxK "+maxk);
        keyOut = new Text();
        valueOut = new Text(maxk+":"+1);
    }
    
    /**
     * 
     * @param itemA
     * @param itemB
     * @param i
     * @param j
     * @return
     */
    public boolean isSamePrefix(String[] itemA, String[] itemB, int i, int j){
    	if(k == 2) return true;
    	for(int a = 0; a < k -2; a++){
            if(!itemA[a].equals(itemB[a])){
            	//System.out.println("Não é o mesmo prefixo: "+itemA[a]+" != "+itemB[a]);
                return false;
            }
        }
        
    	return true;
    }
    
    /**
     * 
     * @param itemA
     * @param itemB
     * @return
     */
    public String combine(String[] itemA, String[] itemB, String[] itemsetCSpt){
        StringBuilder sb = new StringBuilder();
        
        for(int i = 0; i < itemA.length; i++){
            sb.append(itemA[i]).append(" ");
            itemsetCSpt[i] = itemA[i]; 
        }
        sb.append(itemB[itemB.length-1]);
        itemsetCSpt[k-1] = itemB[itemB.length-1];
        return sb.toString();
    }
    
    /**
     * 
     * @param itemset
     * @param frequents
     * @return
     */
    private boolean allSubsetIsFrequent(String[] itemset, ArrayList<String> frequents) { 
		int indexToSkip = 0;
		StringBuilder subItem;
		for(int j = 0; j < itemset.length-1; j++){
			subItem = new StringBuilder();
			for(int i = 0; i < itemset.length; i++){
				if(i != indexToSkip){
					subItem.append(itemset[i]).append(" ");
				}
			}
			//subItem gerado, verificar se é do conjunto frequente
			
			if(!frequents.contains(subItem.toString().trim())){
				return false;
			}
			indexToSkip++;
		}
		
		return true;
	}
    
    /**
     * 
     * @param transaction
     * @param pt
     * @param i
     * @param k
     * @param itemset
     * @param itemsetIndex
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
		boolean endBlock = false;
		int pos;
		int start = 0;
		int len;
		String[] itemset;
		while ((pos = value.find("\n", start)) != -1) {
			k = mink;
			len = pos - start;
			try {
				transaction = Text.decode(value.getBytes(), start, len).trim().split(" ");
				for(;k <= maxk; k++){
			    	if(transaction.length >= k){
			    		for(int i = 0; i < transaction.length; i++){
			    			itemset = new String[k];
			    			subSet(transaction, hpt.getHashNode(), i, itemset, 0, context);
			    		}
			    	}
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
				for(;k <= maxk; k++){
			    	if(transaction.length >= k){
			    		for(int i = 0; i < transaction.length; i++){
			    			itemset = new String[k];
			    			subSet(transaction, hpt.getHashNode(), i, itemset, 0, context);
			    		}
			    	}
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
    public ArrayList<String> openFile(String path, Context context){
    	
    	try {
			
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), context.getConfiguration());
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), context.getConfiguration());
			
			while (reader.next(key, value)) {
				//System.out.println("Add Key: "+key.toString());
	            fileCached.add(key.toString());
	        }
			long begin = System.currentTimeMillis();
			Collections.sort(fileCached, NUMERIC_ORDER);
			long end = System.currentTimeMillis();
			double total = end-begin;
			if((total/1000) > 1){
				System.out.println("Tempo gasto para ordenação: "+total+" milis ou "+total/1000+" segundos...");
			}
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return fileCached;
    }
    
    public static Comparator<Object> NUMERIC_ORDER = new Comparator<Object>() {
    	public int compare(Object obj1, Object obj2){
    		
    		String[] o1 = ((String)obj1).trim().split(" ");
    		String[] o2 = ((String)obj2).trim().split(" ");
    		int a;
    		int b;
    		for(int i = 0; i < o1.length; i++){
    			a = Integer.parseInt(o1[i]);
    			b = Integer.parseInt(o2[i]);
    			
    			if(a < b){
    				return -1;
    			}else if(a > b){
    				return 1;
    			}else{
    				continue;
    			}
    		}
    		
    		return 0;
    	}
	};
}
