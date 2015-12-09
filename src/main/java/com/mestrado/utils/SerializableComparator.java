package main.java.com.mestrado.utils;

import java.io.Serializable;
import java.util.Comparator;

public class SerializableComparator implements Comparator<String>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(String o1, String o2) {
		String[] item1 = o1.split(" ");
		String[] item2 = o2.split(" ");
		
		for(int i = 0; i < item1.length; i++){
			if(Integer.parseInt(item1[i]) < Integer.parseInt(item2[i])){
				return -1;
			}else if(Integer.parseInt(item1[i]) > Integer.parseInt(item2[i])){
				return 1;
			}
		}
		return 0;
	}

}
