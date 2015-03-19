package app;

import java.util.ArrayList;
import java.util.Arrays;

public class PrefixTree {
	private ArrayList<String> prefix;
	private ArrayList<PrefixTree> prefixTree;
	private int level;
	public PrefixTree(int level) {
		this.level = level;
		prefix = new ArrayList<String>();
		prefixTree = new ArrayList<PrefixTree>();
	}
	public ArrayList<String> getPrefix() {
		return prefix;
	}
	public void setPrefix(ArrayList<String> prefix) {
		this.prefix = prefix;
	}
	public ArrayList<PrefixTree> getPrefixTree() {
		return prefixTree;
	}
	public void setPrefixTree(ArrayList<PrefixTree> prefixTree) {
		this.prefixTree = prefixTree;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	
	/**
	 * 
	 * @param pt prefix tree object
	 * @param itemset array
	 * @param i always zero
	 */
	public void add(PrefixTree pt, String[] itemset, int i){
		if(i >= itemset.length){
			return;
		}
		
		/*Verificar se o item está na lista da tree*/
//		System.out.println("Prefix: "+pt.prefix+", item: "+itemset[i]);
		int index = pt.prefix.indexOf(itemset[i]);
//		System.out.println(pt.prefix+", itemset "+new ArrayList(Arrays.asList(itemset))+", i = "+i+", index = "+index);
		if(index != -1){
			
			/*Existe o item, percorrer a árvore de acordo com o index obtido*/
			i++;
			if(pt.prefixTree.size() <= index || pt.prefixTree.get(index) == null){
				
				for(int x = pt.prefixTree.size(); x <= index; x++){
					pt.prefixTree.add(x, null);
				}
				
				pt.prefixTree.add(index, new PrefixTree(i));
//			}else if(pt.prefixTree.get(index) == null){
//				pt.prefixTree.add(index, new PrefixTree(i));
			}
			add(pt.prefixTree.get(index), itemset, i);
		}else{
			 
			/*Adicionar o novo item no nó*/
			index = pt.prefix.size();
			pt.prefix.add(index,itemset[i]);
			i++;
			if(i >= itemset.length){
				return;
			}
			pt.prefixTree.add(index, new PrefixTree(i));
			add(pt.prefixTree.get(index), itemset, i);
		}
	}
	
	public void find(PrefixTree pt, String[] transaction, int i, int k, String[] itemset, int itemsetIndex){
		
		if(i >= transaction.length){
			return;
		}
		int index = pt.prefix.indexOf(transaction[i]);
		
		if(index == -1){
			System.out.println("Não achou1 :(");
			return;
		}else{
			itemset[itemsetIndex] = transaction[i];
			itemsetIndex++;
			if(i == transaction.length-1){
//				sb.append(transaction[i]);
				System.out.println("Achou1 :) "+Arrays.asList(itemset));
				//itemset[itemsetIndex] = "";
				return;
			}else{
//				sb.append(transaction[i]).append(" ");
				i++;
				if(pt.level == k-1){
					System.out.println("Achou2 :) "+Arrays.asList(itemset));
					//itemset[itemsetIndex] = "";
					return;
				}
				if(pt.prefixTree.isEmpty()){
					System.out.println("Não achou2 :'(");
					return;
				}else{
					while(i < transaction.length){
						find(pt.prefixTree.get(index),transaction,i, k, itemset, itemsetIndex);
						System.out.println("Retornou i = "+i);
						i++;
					}
				}
			}
		}
	}
	
	public void printStrArray(ArrayList<String> str){
		System.out.println("Array de itemsets");
		for(String s: str){
			System.out.println(s);
		}
		System.out.println();
	}
	public void printPrefixTree(PrefixTree pt){
		System.out.println("Imprime prefixTree");
		if(!pt.prefix.isEmpty()){
			System.out.println("Level: "+pt.level);
			printStrArray(pt.prefix);
			
			for(int i = 0; i < pt.prefixTree.size(); i++){
				printPrefixTree(pt.prefixTree.get(i));
			}
		}
	}
	
	public static void main(String[] args){
		PrefixTree pt = new PrefixTree(0);
		String[] itemset1 = {"1", "2"};
		String[] itemset2 = {"1", "3"};
		String[] itemset3 = {"1", "4"};
		String[] itemset4 = {"2", "3"};
		String[] itemset5 = {"2", "4"};
		String[] itemset6 = {"3", "4"};
		String[] itemset7 = {"1", "2", "3"};
		String[] itemset8 = {"1", "2", "4"};
		String[] itemset9 = {"1", "3", "4"};

		pt.add(pt, itemset1, 0);
		pt.add(pt, itemset2, 0);
		pt.add(pt, itemset3, 0);
		pt.add(pt, itemset4, 0);
		pt.add(pt, itemset5, 0);
		pt.add(pt, itemset6, 0);
		pt.add(pt, itemset7, 0);
		pt.add(pt, itemset8, 0);
		pt.add(pt, itemset9, 0);
		pt.printPrefixTree(pt);
		
		/*for(int i = 0; i < itemset4.length; i++)
			pt.find(pt, itemset4, i, 3, new String[3], 0);*/
	}
}