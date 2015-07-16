package main.java.com.mestrado.app;

import java.util.HashMap;

public class HashNode {
	private HashMap<String, HashNode> hashNode;
	private int level;

	public HashNode(HashMap<String, HashNode> hashNode, int level) {
		this.hashNode = hashNode;
		this.level = level;
	}

	public HashNode(int level) {
		this.hashNode = new HashMap<String,HashNode>();
		this.level = level;
	}

	public HashMap<String, HashNode> getHashNode() {
		return hashNode;
	}

	public void setHashNode(HashMap<String, HashNode> hashNode) {
		this.hashNode = hashNode;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}
	
//	public HashNode get(String key){
//		return hashNode.get(key);
//	}
}
