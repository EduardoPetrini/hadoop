package main.java.com.mestrado.app;

import java.util.HashMap;

public class HashNode {
	public HashMap<String, HashNode> hashNode;
	int level;

	public HashNode(HashMap<String, HashNode> hashNode, int level) {
		this.hashNode = hashNode;
		this.level = level;
	}

	public HashNode(int level) {
		this.hashNode = new HashMap<String,HashNode>();
		this.level = level;
	}
}
