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
import java.util.Set;

import main.java.com.mestrado.app.PrefixTree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
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
    private HashMap<String, Integer> itemSup;
    
    private PrefixTree prefixTree;
    String splitName;
    
    
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
    	prefixTree = new PrefixTree(0);
    	System.out.println("Procentagem do suporte: "+support+"%");
    	
    	int k = 1;
    	String[] itemset;
    	String[] tr;
    	int itemsetIndex;
    	boolean endBlock = false;
    	int pos;
    	int start = 0;
    	int len;
    	do{
    		System.out.println("Gerando itens de tamanho "+k);
    		generateCandidates(key, k, value, context);
    		
    		//Verificar existência e contar o support de cada itemset
    		if(k > 1){
    			while((pos = value.find("\n",start)) != -1){
    				len = pos-start;
    				try {
    					tr = Text.decode(value.getBytes(), start, len).split(" ");
    					for(int i = 0; i < tr.length; i++){
    						itemset = new String[k];
	    					itemsetIndex = 0;
	    					subSet(tr, prefixTree, i, k, itemset, itemsetIndex);
    					}
    				} catch (CharacterCodingException e) {
    					e.printStackTrace();
    					System.exit(1);
    				}
    				start = pos+1;
    				if(start >= value.getLength()){
    					System.out.println("Break... "+value.getLength());
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
	    					subSet(tr, prefixTree, i, k, itemset, itemsetIndex);
    					}
    				} catch (CharacterCodingException e) {
    					e.printStackTrace();
    					System.exit(1);
    				}
    			}
    			//limpar prefixTree
    			prefixTree = new PrefixTree(0);
//    			/*Adicionar os itemsets frequentes e os envia para o Reduce*/
    			addFrequentsItemsAndSendToReduce(context);
    		}
    		
    		k++;
    	}while(frequents.size() > 1);
    }
    
    public void setSplitName(LongWritable offset, int blockSize){
    	
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
    private void subSet(String[] transaction, PrefixTree pt, int i,
			int k, String[] itemset, int itemsetIndex) {
    	if(i >= transaction.length){
			return;
		}
		int index = -1;
		
		try{
			index = pt.getPrefix().indexOf(transaction[i]);
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Prefix: "+pt.getPrefix()+" item: "+transaction[i]);
			System.out.println("Error");
		}
		
		if(index == -1){
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			/*if(i == transaction.length-1 && i == k-1){
				StringBuilder sb = new StringBuilder();
				System.out.println("Achou1 "+Arrays.asList(itemset));
				for(String s: itemset){
					if(s != null)
						sb.append(s).append(" ");
				}
				
				itemset[itemsetIndex] = "";
				addToHashItemSup(sb.toString().trim());
				return;
			}else{*/
				i++;
				if(pt.getLevel() == k-1){
					StringBuilder sb = new StringBuilder();
//					System.out.println("Achou2 "+Arrays.asList(itemset));
					for(String s: itemset){
						if(s != null)
							sb.append(s).append(" ");
					}
					addToHashItemSup(sb.toString().trim());
					itemset[itemsetIndex] = "";
					return;
				}
				
				if(pt.getPrefixTree().isEmpty() || pt.getPrefixTree().size() <= index || pt.getPrefixTree().get(index) == null){
					itemset[itemsetIndex] = "";
					return;
				}else{
					itemsetIndex++;
					while(i < transaction.length){
						subSet(transaction, pt.getPrefixTree().get(index),i, k, itemset, itemsetIndex);
						i++;
					}
				}
				
//			}
		}
	}
	public void generateCandidates(LongWritable offset, int n, Text value, Context context){
		System.out.println("Valor de N: "+n);
    	
    	StringBuilder tmpItem;
    			
    	if(n==1){
    		frequents.clear();
    		generateCandidates1(offset, value);
    		
    		System.out.println("Quantidade de candidatos de tamanho 1: "+frequents.size());
    		removeUnFrequentItemsAndSendToReduce(context);
    		Collections.sort(frequents, NUMERICAL_ORDER);
        	
        	System.out.println("Quantidade de frequentes de tamanho "+n+": "+frequents.size());
    		
    	}else if(n==2) {
    		for(int i=0; i<frequents.size(); i++){
    			tmpItem = new StringBuilder();
    			tmpItem.append(frequents.get(i).trim()).append(" ");
    			for(int j=i+1; j<frequents.size(); j++){
    				prefixTree.add(prefixTree,(tmpItem.toString()+frequents.get(j).trim()).split(" "),0);
    			}
    		}
    		frequents.clear();
    	}else{
    		/*É preciso verificar o prefixo, isso não está sendo feito!!*/
    		String prefix;
    		String sufix;
    		String newItemSet;
    		for(int i=0; i<frequents.size(); i++){
//    			System.out.println("Progress: "+context.getProgress());
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
	    		
	    					//add to prefixTree
	    					//System.out.println("tmpSize in K = "+n+": candidate size "+tempCandidates.size()+", Adicionando na hash "+tempCandidates.get(tempCandidates.size()-1));
	    					try{
	    						prefixTree.add(prefixTree,newItemSet.split(" "),0);
	    					}catch(Exception e){
	    						e.printStackTrace();
	    					}
    					}
    				}
    			}
    		}
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
		//avbdgatai
		
		return true;
	}
	/**
	 * Encontra os frequentes de tamanho 1
	 * @param tempCandidates2
	 * @param value
	 */
	private void generateCandidates1(LongWritable offset, Text value) {
		String[] tmpItemsets;
		int start = 0;
    	int len;
		int pos;
		itemSup = new HashMap<String, Integer>();
		int blockSize = 0;
		boolean endBlock = false;
		while((pos = value.find("\n",start)) != -1){
			len = pos-start;
			blockSize++;
			try {
				tmpItemsets = Text.decode(value.getBytes(), start, len).split(" ");
				for(int j = 0; j < tmpItemsets.length; j++){
					if(addToHashItemSup(tmpItemsets[j])){
						frequents.add(tmpItemsets[j]);		
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
			start = pos+1;
			if(start >= value.getLength()){
				System.out.println("Break... "+value.getLength());
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
					if(addToHashItemSup(tmpItemsets[j])){
						frequents.add(tmpItemsets[j]);		
					}
				}
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		//A partir daqui já te sabe o tamanho do bloco
		support = Math.ceil(support * blockSize);
		setSplitName(offset, blockSize);
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
	
	public boolean addToHashItemSup(String item){
    	Integer value = 0;
    	
    	if((value = itemSup.get(item)) == null){
    		itemSup.put(item, 1);
    		return true;
    	}else{
    		value++;
    		itemSup.put(item, value);
    		return false;
    	}
    }
    
    /**
     * Remove itemsets não frequentes e envia para o reduce
     * @param context
     * @param tempCandidates
     */
    public void removeUnFrequentItemsAndSendToReduce(Context context){
    	Integer value;
    	ArrayList<String> rmItems = new ArrayList<String>();
    	Text key = new Text();
		Text val = new Text();
    	for(String item: frequents){
    		value = itemSup.get(item);
    		System.out.println("item: "+item+" sup: "+value);
    		if(value == null || value < support){
    			rmItems.add(item);
    		}else{
    			//envia para o reduce
    			key.set(item);
    			val.set(String.valueOf(value)+":"+splitName);
    			try {
					context.write(key, val);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
    		}
    	}
    	
    	System.out.println("\nRemovendo "+rmItems.size()+" não frequentes.\n");
    	itemSup.clear();
    	frequents.removeAll(rmItems);
    }
    
    /**
     * Adiciona os itemsets frequentes da hash no vetor frequents para gerar Lk
     * @param context
     */
    public void addFrequentsItemsAndSendToReduce(Context context){
    	Integer value;
    	ArrayList<String> rmItems = new ArrayList<String>();
    	Text key = new Text();
		Text val = new Text();
		Set<String> keys = itemSup.keySet();
    	for(String item: keys){
    		value = itemSup.get(item);
    		
    		if(value == null || value < support){
    			rmItems.add(item);
    		}else{
    			//envia para o reduce
    			frequents.add(item);
    			key.set(item);
    			val.set(String.valueOf(value)+":"+splitName);
    			try {
					context.write(key, val);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
    		}
    	}
    	itemSup.clear();
    	
    	System.out.println("\nRemovendo "+rmItems.size()+" não frequentes.\n");
    	
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