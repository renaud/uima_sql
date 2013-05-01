package de.uniwue.misc.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

public class EncodingTransformer {



	public static void transform(File inputDir, File outputDir) {
		FileInputStream fstream;
		for (File aFile : inputDir.listFiles()) {
			try {
				fstream = new FileInputStream(aFile);
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(new InputStreamReader(in, "Windows-1252"));
				File outputFile = new File(outputDir, aFile.getName());
				FileOutputStream outstream = new FileOutputStream(outputFile);
				DataOutputStream out = new DataOutputStream(outstream);
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
				String aLine;
				long counter = 0;
				while ((aLine = br.readLine()) != null)   {
					aLine = aLine.replaceAll("\\|", "\t");
					writer.write(aLine + "\n");
					counter++;
					if (counter % 5000 == 0) {
						System.out.println(counter);
						//        break;
					}
				}
				writer.close();
				br.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


}
