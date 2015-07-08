package main.java.com.mestrado.app;


public class HashPrefixTree {
	private HashNode hashNode;
	
	public HashPrefixTree() {
		this.hashNode = new HashNode(0);
	}
	
	public HashNode getHashNode() {
		return hashNode;
	}

	public void setHashNode(HashNode hashNode) {
		this.hashNode = hashNode;
	}

	/**
	 * @param pt prefix tree object
	 * @param itemset array
	 * @param i always zero
	 */
	public void add(HashNode hNode, String[] itemset, int i){
		if(i >= itemset.length){
			return;
		}
		
		HashNode son = hNode.hashNode.get(itemset[i]);
		
		if(son == null){
			//Cria um novo registro na hash
			hNode.hashNode.put(itemset[i], new HashNode(i));
			i++;
			if(i >= itemset.length){
				return;
			}
			add(hNode.hashNode.get(itemset[i]), itemset, i);
		}else{
			//percorre a hashTree
			i++;
			add(son,itemset,i);
		}
	}
	
	public static void main(String[] args) {
		HashPrefixTree hpt = new HashPrefixTree();
		String[] itemset = {"a","b","c"};
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[1] = "c";
		itemset[2] = "d";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[0] = "b";
		itemset[1] = "c";
		itemset[2] = "d";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[0] = "b";
		itemset[1] = "c";
		itemset[2] = "e";
		hpt.add(hpt.getHashNode(), itemset, 0);
		itemset[1] = "c";
		itemset[2] = "e";
		hpt.add(hpt.getHashNode(), itemset, 0);
		System.out.println("Fim");
	}
}