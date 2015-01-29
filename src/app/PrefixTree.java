package app;

import java.util.ArrayList;

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
	
	public void add(PrefixTree pt, String[] itemset, int i){
		if(i >= itemset.length){
			return;
		}
		
		/*Verificar se o item está na lista da tree*/
		
		int index = pt.prefix.indexOf(itemset[i]);
		if(index != -1){
			
			/*Existe o item, percorrer a árvore de acordo com o index obtido*/
			i++;
			if(pt.prefixTree.isEmpty()){
				pt.prefixTree.add(index, new PrefixTree(i));
				add(pt.prefixTree.get(index), itemset, i);
			}else{
				add(pt.prefixTree.get(index), itemset, i);
			}
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
	
	public void find(PrefixTree pt, String[] itemset, int i, StringBuilder sb){
		
		if(i >= itemset.length){
			return;
		}
		int index = pt.prefix.indexOf(itemset[i]);
		
		if(index == -1){
			System.out.println("Não achou :(");
			return;
		}else{
			if(i == itemset.length-1){
				sb.append(itemset[i]);
				System.out.println("Achou :) "+sb.toString());
				return;
			}else{
				sb.append(itemset[i]).append(" ");
				i++;
				if(pt.prefixTree.isEmpty()){
					System.out.println("Não achou :'(");
					return;
				}else{
					find(pt.prefixTree.get(index),itemset,i, sb);
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
		String[] itemset = {"a","b","c","d"};
		String[] itemset2 = {"a","b","c","d", "e"};
		String[] itemset3 = {"k","x","z"};
		pt.add(pt, itemset, 0);
//		pt.add(pt, itemset2, 0);
		pt.add(pt, itemset3, 0);
		pt.printPrefixTree(pt);
		pt.find(pt, itemset2, 0, new StringBuilder());
	}
}
