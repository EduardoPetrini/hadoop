package app;

import java.util.ArrayList;

public class PrefixTree {
	private ArrayList<String> prefix;
	private ArrayList<PrefixTree> prefixTree;
	private int k;
	private int level;
	private int prefixSize;
	public PrefixTree(int k, int level, int prefixSize) {
		this.k = k;
		this.level = level;
		this.prefixSize = prefixSize;
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
	public int getK() {
		return k;
	}
	public void setK(int k) {
		this.k = k;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public int getPrefixSize() {
		return prefixSize;
	}
	public void setPrefixSize(int prefixSize) {
		this.prefixSize = prefixSize;
	}
	
	public void add(String itemset){
		
	}
	
	public void find(String transaction){
		
	}
	
	public static void main(String[] args){
		PrefixTree pt = new PrefixTree(3, 0, 1);
	}
}
