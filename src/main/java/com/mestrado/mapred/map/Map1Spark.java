
package main.java.com.mestrado.mapred.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import main.java.com.mestrado.app.HashNode;
import main.java.com.mestrado.app.HashPrefixTree;

/**
 *
 * @author eduardo
 */
public class Map1Spark implements Function2<Integer, Iterator<String>, Iterator<String>> {

	private static final long serialVersionUID = 1L;
	private double support;
	private ArrayList<String> frequents;
	private HashMap<String, Integer> itemSupHash;
	private ArrayList<String> newFrequents;
	private List<String> chaveValues;

	private HashPrefixTree hpt;
	private String splitName;
	private long blockSize;

	public Map1Spark(Broadcast<Double> broadSup) {
		support = broadSup.value();
	}

	@Override
	public Iterator<String> call(Integer blockIndex, Iterator<String> blockContent) throws Exception {
		System.out.println("\n*****************/////// KEY: " + blockIndex);
		frequents = new ArrayList<String>();
		newFrequents = new ArrayList<String>();
		itemSupHash = new HashMap<String, Integer>();
		hpt = new HashPrefixTree();
		chaveValues = new ArrayList<String>();
		System.out.println("Support rate: " + support);

		int k = 1;
		String[] itemset;
		String[] tr;
		int itemsetIndex;

		do {
			System.out.println("Gerando itens de tamanho " + k);
			generateCandidates(blockIndex, k, blockContent);

			if (k > 1) {
				System.out.println("Iniciando a verificação dos candidatos C" + k);
				while (blockContent.hasNext()) {
					tr = blockContent.next().split(" ");
					for (int i = 0; i < tr.length; i++) {
						itemset = new String[k];
						itemsetIndex = 0;
						subSet(tr, hpt.getHashNode(), i, k, itemset, itemsetIndex);
					}
				}
				// limpar prefixTree
				hpt = new HashPrefixTree();
				// /*Adicionar os itemsets frequentes e os envia para o Reduce*/
				System.out.println("Enviando os L" + k + " itemsets frequentes para Reduce...");
				addFrequentsItemsAndSendToReduce(k);
			}

			k++;

		} while (frequents.size() > 1);

		return chaveValues.iterator();
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
				addItemsetToItemSupHash(sb.toString().trim());
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
    
private void addItemsetToItemSupHash(String itemset){
    	
    	Integer value = itemSupHash.get(itemset);
    	if(value != null){
    		value++;
    	}else{
    		value = new Integer(1);
    	}
    	itemSupHash.put(itemset, value);
    }

	public void generateCandidates(Integer blockIndex, int k, Iterator<String> blockContent) {
		System.out.println("Valor de K: " + k);

		StringBuilder tmpItem;

		if (k == 1) {
			HashMap<String, Integer> itemSupHash = new HashMap<String, Integer>();
			generateCandidates1(blockIndex, blockContent, itemSupHash);

			// System.out.println("Gerados "+frequents.size()+" candidatos de
			// tamanho "+n);
			// removeUnFrequentItemsAndSendToReduce(itemSupHash);
			Collections.sort(frequents, NUMERICAL_ORDER);

		} else if (k == 2) {
			String item;
			for (int i = 0; i < frequents.size(); i++) {
				tmpItem = new StringBuilder();
				tmpItem.append(frequents.get(i).trim()).append(" ");
				for (int j = i + 1; j < frequents.size(); j++) {
					item = tmpItem.toString() + frequents.get(j);
					newFrequents.add(item);
					itemSupHash.put(item, 1);
					hpt.add(hpt.getHashNode(), item.split(" "), 0);
				}
			}
			System.out.println("Gerados " + newFrequents.size() + " candidatos de tamanho " + k);
			frequents.clear();
		} else {
			/* É preciso verificar o prefixo, isso não está sendo feito!! */
			String prefix;
			String sufix;
			String newItemSet;
			int count = 0;
			// ItemSup item;
			for (int i = 0; i < frequents.size(); i++) {
				// // System.out.println("Progress: "+context.getProgress());
				for (int j = i + 1; j < frequents.size(); j++) {

					prefix = getPrefix(frequents.get(i));

					if (frequents.get(j).startsWith(prefix)) {
						/*
						 * Se o próximo elemento já possui o mesmo prefixo,
						 * basta concatenar o sufixo do segundo item.
						 */
						sufix = getSufix(frequents.get(j));

						tmpItem = new StringBuilder();
						tmpItem.append(frequents.get(i)).append(" ").append(sufix);
						// tmpItem é o novo candidato, verificar e todo o seu
						// subconjunto é frequente
						newItemSet = tmpItem.toString().trim();
						if (allSubsetIsFrequent(newItemSet.split(" "))) {
							count++;
							// item = new ItemSup(newItemSet,0);
							newFrequents.add(newItemSet);
							itemSupHash.put(newItemSet, 1);
							try {
								hpt.add(hpt.getHashNode(), newItemSet.split(" "), 0);
							} catch (Exception e) {
								e.printStackTrace();
							}
							if (count % 2000 == 0) {
								System.out.println("Para k = " + k + " gerando " + count + " itemsets... ultimo: " + newItemSet);
							}
						}
					}
				}
			}
			// System.out.println("Gerados "+count+" candidatos de tamanho "+n);
			frequents.clear();
		}
	}

	private void generateCandidates1(Integer blockIndex, Iterator<String> blockContent, HashMap<String, Integer> itemSupHash) {
		String[] tmpItemsets;
		blockSize = 0;
		boolean endBlock = false;
		while (blockContent.hasNext()) {
			blockSize++;
			tmpItemsets = blockContent.next().split(" ");
			for (int j = 0; j < tmpItemsets.length; j++) {
				if (addItemsetToItemSupHash(tmpItemsets[j], itemSupHash)) {
					frequents.add(tmpItemsets[j]);
				}
			}
		}

		setSplitName(blockIndex);
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

	private boolean addItemsetToItemSupHash(String itemset, HashMap<String, Integer> itemSupHash) {
		Integer value = itemSupHash.get(itemset);
		if (value == null) {
			itemSupHash.put(itemset, 1);
			return true;
		} else {
			itemSupHash.put(itemset, value + 1);
		}
		return false;
	}

	public String getSufix(String kitem) {
		String[] spkitem = kitem.split(" ");
		return spkitem[spkitem.length - 1].trim();
	}

	public String getPrefix(String kitem) {

		String[] spkitem = kitem.split(" ");
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < spkitem.length - 1; i++) {

			sb.append(spkitem[i]).append(" ");
		}

		// k = spkitem.length;
		return sb.toString();
	}

	/**
	 * Para L1 Remove itemsets não frequentes e envia para o reduce
	 * 
	 * @param context
	 * @param tempCandidates
	 */
	public void removeUnFrequentItemsAndSendToReduce(Context context, HashMap<String, Integer> itemSupHash) {
		Integer value;
		ArrayList<String> rmItems = new ArrayList<String>();
		Text key = new Text();
		Text val = new Text();
		double rm;
		for (String item : frequents) {
			value = itemSupHash.get(item);
			rm = (value / ((double) blockSize));
			if (value != null && ((value / ((double) blockSize)) >= support)) {
				// envia para o reduce
				key.set(item);
				val.set(String.valueOf(value) + ":" + splitName);
				try {
					context.write(key, val);
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			} else {
				rmItems.add(item);
			}
		}
		frequents.removeAll(rmItems);
	}

	/**
	 * Para Lk Adiciona os itemsets frequentes da hash no vetor frequents para
	 * gerar Lk
	 * 
	 * @param context
	 */
	public void addFrequentsItemsAndSendToReduce(int k) {
		Integer value;

		StringBuilder keyVal;
		for (String item : newFrequents) {
			value = itemSupHash.get(item);
			if (value != null && ((value / ((double) blockSize)) >= support)) {
				// envia para o reduce
				keyVal = new StringBuilder();
				keyVal.append(item).append("#");
				frequents.add(item);
				keyVal.append(value).append(":").append(splitName);
				chaveValues.add(keyVal.toString());
			}
		}
		newFrequents.clear();
		itemSupHash.clear();
	}

	public void setSplitName(Integer blockIndex) {

		splitName = blockIndex + ":" + blockSize;
		System.out.println("|************************************************************|");
		System.out.println("Split Name: " + splitName + " , support " + support);
		System.out.println("|************************************************************|");

	}

	private static Comparator<Object> NUMERICAL_ORDER = new Comparator<Object>() {
		public int compare(Object ob1, Object ob2) {
			int val1 = Integer.parseInt((String) ob1);
			int val2 = Integer.parseInt((String) ob2);

			return val1 > val2 ? 1 : val1 < val2 ? -1 : 0;
		}
	};
}
