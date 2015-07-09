/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;
import main.java.com.mestrado.app.ItemSup;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author eduardo
 */
public class Map1 extends Mapper<LongWritable, Text, Text, Text>{
    
    private Log log = LogFactory.getLog(Map1.class);
    private double support;
    private ArrayList<String> frequents;
//    private HashMap<String, Integer> itemSup;
    private ArrayList<ItemSup> newFrquents;
    
    private HashPrefixTree hpt;
    private String splitName;
    private long blockSize;
    
    
    @Override
    public void setup(Context context){
    	String sup = context.getConfiguration().get("supportPercentage");
    	support = Double.parseDouble(sup);
    	log.info("Iniciando Map 1...");
    	    	
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException{
    	//Recebe todo o bloco de transações de uma vez
    	//Aplica-se o algoritmo Apriori
    	System.out.println("\n*****************/////// KEY: "+key);

    	frequents = new ArrayList<String>();
    	newFrquents = new ArrayList<ItemSup>();
    	hpt = new HashPrefixTree();
    	System.out.println("Procentagem do suporte: "+support+"%");
    	
    	int k = 1;
    	String[] itemset;
    	String[] tr;
    	int itemsetIndex;
    	boolean endBlock = false;
    	int pos;
    	int start;
    	int len;
    	do{
    		System.out.println("Gerando itens de tamanho "+k);
    		generateCandidates(key, k, value, context);
    		
    		//Verificar existência e contar o support de cada itemset
    		if(k > 1){
    			start = 0;
    			while((pos = value.find("\n",start)) != -1){
    				len = pos-start;
    				try {
    					tr = Text.decode(value.getBytes(), start, len).split(" ");
    					for(int i = 0; i < tr.length; i++){
    						itemset = new String[k];
	    					itemsetIndex = 0;
	    					subSet(tr, hpt.getHashNode(), i, k, itemset, itemsetIndex);
    					}
    				} catch (CharacterCodingException e) {
    					e.printStackTrace();
    					System.exit(1);
    				}
    				start = pos+1;
    				if(start >= value.getLength()){
    					// System.out.println("Break... "+value.getLength());
    					endBlock = true;
    					break;
    				}
    			}
    			//pegar a ultima transação, caso tenha
    			if(!endBlock){
    				len = value.getLength()-start;
    				try {
    					tr = Text.decode(value.getBytes(), start, len).split(" ");
    					for(int i = 0; i < tr.length; i++){
    						itemset = new String[k];
	    					itemsetIndex = 0;
	    					subSet(tr, hpt.getHashNode(), i, k, itemset, itemsetIndex);
    					}
    				} catch (CharacterCodingException e) {
    					e.printStackTrace();
    					System.exit(1);
    				}
    			}
    			//limpar prefixTree
    			hpt = new HashPrefixTree();
//    			/*Adicionar os itemsets frequentes e os envia para o Reduce*/
    			addFrequentsItemsAndSendToReduce(context,k);
    		}
    		
    		k++;
    	}while(frequents.size() > 1);
    }
    
    public void setSplitName(LongWritable offset){
    	
    	splitName = offset+":"+blockSize;
    	System.out.println("|************************************************************|");
    	System.out.println("Split Name: "+splitName+" , support "+support);
    	System.out.println("|************************************************************|");
    	
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
			int k, String[] itemset, int itemsetIndex) {
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
				addItemsetToItemSup(sb.toString().trim());
				itemset[itemsetIndex] = "";
				return;
			}
			
			i++;
			itemsetIndex++;
			while(i < transaction.length){
				subSet(transaction, son, i, k, itemset, itemsetIndex);
				for(int j = itemsetIndex; j < itemset.length; j++){
					itemset[j] = "";
				}
				i++;
			}
		}
	}
	public void generateCandidates(LongWritable offset, int n, Text value, Context context){
		System.out.println("Valor de N: "+n);
    	
    	StringBuilder tmpItem;
    			
    	if(n==1){
    		HashMap<String, Integer> itemSupHash = new HashMap<String, Integer>();
    		generateCandidates1(offset, value, itemSupHash);
    		
    		// System.out.println("Gerados "+frequents.size()+" candidatos de tamanho "+n);
    		removeUnFrequentItemsAndSendToReduce(context, itemSupHash);
    		Collections.sort(frequents, NUMERICAL_ORDER);
        	
    	}else if(n==2) {
    		int count = 0;
    		
    		ItemSup item;
    		for(int i=0; i<frequents.size(); i++){
    			tmpItem = new StringBuilder();
    			tmpItem.append(frequents.get(i).trim()).append(" ");
    			for(int j=i+1; j<frequents.size(); j++){
    				count++;
    				item = new ItemSup(tmpItem.toString()+frequents.get(j).trim(),0);
    				newFrquents.add(item);
    				if(count%2000==0)System.out.println("Para k = "+n+" gerando "+count+" itemsets... ");
    				hpt.add(hpt.getHashNode(),item.getItemset().split(" "),0);
    			}
    		}
    		// System.out.println("Gerados "+count+" candidatos de tamanho "+n);
    		frequents.clear();
    	}else{
    		/*É preciso verificar o prefixo, isso não está sendo feito!!*/
    		String prefix;
    		String sufix;
    		String newItemSet;
    		int count = 0;
    		ItemSup item;
    		for(int i=0; i<frequents.size(); i++){
//    			// System.out.println("Progress: "+context.getProgress());
    			for(int j=i+1; j<frequents.size(); j++){

					prefix = getPrefix(frequents.get(i));
    				
    				if(frequents.get(j).startsWith(prefix)){
    					/*Se o próximo elemento já possui o mesmo prefixo, basta concatenar o sufixo do segundo item.*/
    					sufix = getSufix(frequents.get(j));
    					
						tmpItem = new StringBuilder();
    					tmpItem.append(frequents.get(i)).append(" ").append(sufix);
    					//tmpItem é o novo candidato, verificar e todo o seu subconjunto é frequente
    					newItemSet = tmpItem.toString().trim();
    					if(allSubsetIsFrequent(newItemSet.split(" "))){
    						count++;
    						item = new ItemSup(newItemSet,0);
    						newFrquents.add(item);
	    					try{
	    						hpt.add(hpt.getHashNode(),newItemSet.split(" "),0);
	    					}catch(Exception e){
	    						e.printStackTrace();
	    					}
	    					if(count%2000==0){
	    						System.out.println("Para k = "+n+" gerando "+count+" itemsets... ultimo: "+newItemSet);
	    					}
    					}
    				}
    			}
    		}
    		// System.out.println("Gerados "+count+" candidatos de tamanho "+n);
    		frequents.clear();
    	}
    }
	
	/**
	 * Verifica se todo subconjunto do itemset é frequente
	 * @param itemset
	 * @return
	 */
	private boolean allSubsetIsFrequent(String[] itemset) {
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
	 * Encontra os frequentes de tamanho 1
	 * @param tempCandidates2
	 * @param value
	 */
	private void generateCandidates1(LongWritable offset, Text value, HashMap<String, Integer> itemSupHash) {
		String[] tmpItemsets;
		int start = 0;
    	int len;
		int pos;
		blockSize = 0;
		boolean endBlock = false;
		while((pos = value.find("\n",start)) != -1){
			len = pos-start;
			blockSize++;
			try {
				tmpItemsets = Text.decode(value.getBytes(), start, len).split(" ");
				for(int j = 0; j < tmpItemsets.length; j++){
//					if(j%250==0)System.out.println("Encontrando 1 itemsets... "+j);
					if(addItemsetToItemSupHash(tmpItemsets[j], itemSupHash)){
						frequents.add(tmpItemsets[j]);
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
			start = pos+1;
			if(start >= value.getLength()){
				// System.out.println("Break... "+value.getLength());
				endBlock = true;
				break;
			}
		}
		//pegar a ultima transação
		if(!endBlock){
			len = value.getLength()-start;
			blockSize++;
			try {
				tmpItemsets = Text.decode(value.getBytes(), start, len).split(" ");
				for(int j = 0; j < tmpItemsets.length; j++){
					if(addItemsetToItemSupHash(tmpItemsets[j], itemSupHash)){
						frequents.add(tmpItemsets[j]);
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		//A partir daqui já te sabe o tamanho do bloco
//		support = Math.ceil(support * blockSize);
		setSplitName(offset);
	}
	
	/**
	 * 
	 * @param itemset
	 * @param itemSupHash
	 * @return
	 */
	private boolean addItemsetToItemSupHash(String itemset,
			HashMap<String, Integer> itemSupHash) {
		Integer value = itemSupHash.get(itemset);
		if(value == null){
			itemSupHash.put(itemset, 1);
			return true;
		}else{
			itemSupHash.put(itemset, value+1);
		}
		return false;
	}
	public String getSufix(String kitem){
		String[] spkitem = kitem.split(" ");
		return spkitem[spkitem.length-1].trim();
	}
	
	public String getPrefix(String kitem){
        
        String[] spkitem = kitem.split(" ");
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < spkitem.length-1; i++) {
            
            sb.append(spkitem[i]).append(" ");
        }
        
        //k = spkitem.length;
        return sb.toString();
    }
	
	 /**
     * Remove itemsets não frequentes e envia para o reduce
     * @param context
     * @param tempCandidates
     */
    public void removeUnFrequentItemsAndSendToReduce(Context context, HashMap<String, Integer> itemSupHash){
    	Integer value;
    	ArrayList<String> rmItems = new ArrayList<String>();
    	Text key = new Text();
		Text val = new Text();
		double rm;
    	for(String item: frequents){
    		value = itemSupHash.get(item);
    		rm = (value/((double) blockSize));
    		if(value != null && ((value/((double) blockSize)) >= support)){
    			//envia para o reduce
    			key.set(item);
    			val.set(String.valueOf(value)+":"+splitName);
    			try {
					context.write(key, val);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
    		}else{
    			rmItems.add(item);
    		}
    	}
    	frequents.removeAll(rmItems);
    }
    
    /**
     * Adiciona os itemsets frequentes da hash no vetor frequents para gerar Lk
     * @param context
     */
    public void addFrequentsItemsAndSendToReduce(Context context, int k){
    	Integer value;
    	
    	Text key = new Text();
		Text val = new Text();
		double rm;
    	for(ItemSup item: newFrquents){
    		value = item.getSupport();
    		rm = (value/((double) blockSize));
    		if(value != null && ((value/((double) blockSize)) >= support)){
    			//envia para o reduce
    			frequents.add(item.getItemset());
    			key.set(item.getItemset());
    			val.set(String.valueOf(value)+":"+splitName);
    			try {
					context.write(key, val);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
    		}
    	}
    	newFrquents.clear();
    	// System.out.println("Encontrados "+frequents.size()+" frequentes de tamanho "+k);
    }
    
    private void addItemsetToItemSup(String itemset){
    	ItemSup itemTmp = new ItemSup(itemset);
    	int i;
    	if((i = newFrquents.indexOf(itemTmp)) > -1){
    		newFrquents.get(i).increSupport();
    	}else{
    		newFrquents.add(itemTmp);
    	}
    }
    
    @Override
    public void cleanup(Context context){
    	try {
			super.cleanup(context);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private static Comparator<Object> NUMERICAL_ORDER = new Comparator<Object>()  {
		public int compare(Object ob1, Object ob2) {
			int val1 = Integer.parseInt((String)ob1);
			int val2 = Integer.parseInt((String)ob2);
			
			return val1 > val2? 1: val1 < val2? -1 : 0;
		}
	};
	
	public void printCadidates(ArrayList<String> can, int n){
		System.out.println("\n*************************************\n");
		System.out.println("Print "+n+"-itemsets frequents: ");
		if(!can.isEmpty()){
			for(String s: can){
				System.out.println(s);
			}
			System.out.println("Size: "+can.size());
			System.out.println("k: "+can.get(can.size()-1).split(" ").length);
		}else{
			System.out.println("Nenhum candidato!");
		}
		System.out.println("\n*************************************\n");
	}
}
