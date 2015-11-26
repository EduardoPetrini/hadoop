package main.java.com.mestrado.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import main.java.com.mestrado.main.MainSpark;

public class CountItemsets {
	private static Integer itemCounts[];

	private static void countByOutputDir(String outputPath) {
		Path path = new Path(outputPath);

		Configuration c = new Configuration();

		try {
			c.set("fs.defaultFS", MainSpark.clusterUrl);
			FileSystem fs = FileSystem.get(c);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line;
			String[] lineSpt;
			while ((line = br.readLine()) != null) {
				lineSpt = line.replaceAll("\\(||\\)", "").replaceAll(",.*", "").split(" ");
				if (itemCounts[lineSpt.length - 1] == null) {
					itemCounts[lineSpt.length - 1] = new Integer(1);
					// sbs[lineSpt.length-1] = new StringBuilder();
					// sbs[lineSpt.length-1].append(l).append("\n");
				} else {
					itemCounts[lineSpt.length - 1]++;
					// sbs[lineSpt.length-1].append(l).append("\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String countItemsets(String... filesNames) {
		itemCounts = new Integer[20];

		for (String dirName : filesNames) {
			ArrayList<String> outputFiles = MrUtils.getAllOuputFilesNames(dirName);
			for (String outFile : outputFiles) {
				System.out.println("Contando itemsets em " + outFile);
				CountItemsets.countByOutputDir(outFile);
			}
		}
		int total = 0;
		StringBuilder sb = new StringBuilder();
		sb.append("\n|**********************************|\n\n");
		for (int i = 0; i < itemCounts.length; i++) {
			if (itemCounts[i] != null) {
				total += itemCounts[i];
				sb.append("**********  ").append((i + 1)).append(": ").append(itemCounts[i]).append("\n");
			}
		}
		sb.append("total: ").append(total);
		sb.append("\n\n|**********************************|\n\n");
		return sb.toString();
	}
}