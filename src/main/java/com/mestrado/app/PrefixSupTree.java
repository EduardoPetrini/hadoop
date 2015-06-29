package main.java.com.mestrado.app;

import java.util.ArrayList;

import main.java.com.mestrado.mapred.map.Map2;

public class PrefixSupTree {
	private ArrayList<String> prefix;
	private ArrayList<PrefixSupTree> prefixSupTree;
	private ArrayList<Integer> support;
	private int level;
	
	public PrefixSupTree(int level) {
		this.level = level;
		support = new ArrayList<Integer>();
		prefix = new ArrayList<String>();
		prefixSupTree = new ArrayList<PrefixSupTree>();
	}
	public ArrayList<String> getPrefix() {
		return prefix;
	}
	public void setPrefix(ArrayList<String> prefix) {
		this.prefix = prefix;
	}
	public ArrayList<PrefixSupTree> getPrefixTree() {
		return prefixSupTree;
	}
	public void setPrefixTree(ArrayList<PrefixSupTree> prefixTree) {
		this.prefixSupTree = prefixTree;
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
	public void add(PrefixSupTree pt, String[] itemset, int i){
		if(i >= itemset.length){
			//adicionar o suporte
			int index = pt.prefix.indexOf(itemset[i-1]);
//			pt.support.add(index, Map2.currentSupport);
			return;
		}
		
		/*Verificar se o item está na lista da tree*/
//		System.out.println("Prefix: "+pt.prefix+", item: "+itemset[i]);
		int index = pt.prefix.indexOf(itemset[i]);
//		System.out.println(pt.prefix+", itemset "+new ArrayList(Arrays.asList(itemset))+", i = "+i+", index = "+index);
		if(index != -1){
			
			/*Existe o item, percorrer a árvore de acordo com o index obtido*/
			i++;
			if(pt.prefixSupTree.size() <= index || pt.prefixSupTree.get(index) == null){
				
				for(int x = pt.prefixSupTree.size(); x <= index; x++){
					pt.prefixSupTree.add(x, null);
				}
				
				pt.prefixSupTree.add(index, new PrefixSupTree(i));
//			}else if(pt.prefixTree.get(index) == null){
//				pt.prefixTree.add(index, new PrefixTree(i));
			}
			add(pt.prefixSupTree.get(index), itemset, i);
		}else{
			 
			/*Adicionar o novo item no nó*/
			index = pt.prefix.size();
			pt.prefix.add(index,itemset[i]);
			i++;
			if(i >= itemset.length){
				//adicionar o suporte
//				pt.support.add(index,Map2.currentSupport);
				return;
			}
			pt.prefixSupTree.add(index, new PrefixSupTree(i));
			add(pt.prefixSupTree.get(index), itemset, i);
		}
	}
	
	public void find(PrefixSupTree pt, String[] transaction, int i, String[] itemset, int itemsetIndex){
		
		
	}
	
	public void printStrArray(ArrayList<String> str){
		System.out.println("Array de itemsets");
		for(String s: str){
			System.out.print(s+" ");
		}
		System.out.println();
	}
	public void printPrefixTree(PrefixSupTree pt){
		if(pt == null) return;
		System.out.println("Imprime prefixTree");
		if(!pt.prefix.isEmpty()){
			System.out.println("Level: "+pt.level);
			printStrArray(pt.prefix);
			
			for(int i = 0; i < pt.prefixSupTree.size(); i++){
				printPrefixTree(pt.prefixSupTree.get(i));
			}
		}
	}
	
	public static void main(String[] args){
		PrefixSupTree pt = new PrefixSupTree(0);
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
		
		for(int i = 0; i < itemset4.length; i++)
			pt.find(pt, itemset4, i, new String[3], 0);
	}
}