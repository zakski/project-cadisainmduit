package com.szadowsz.grainne.tools.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

public class FWriter {

	protected String encoding; // encoding of the file to write to
	protected String fileName; // the path of the file to write to
	protected boolean append; //true if appending, false if overwriting
	protected BufferedWriter bWriter; // buffered writer to do the writing

	/**
	 * Simple Constructor that sets key variables of the writer
	 * 
	 * @param nFile the filepath to write to
	 * @param encode the encoding of the file
	 * @param append whether or not we should append the lines to the end of the file
	 * @throws FileNotFoundException
	 */
	public FWriter(String nFile, String encode, boolean append) {
		fileName = nFile;
		encoding = encode;
		this.append = append;
	}

	/**
	 * Writes a line out to a file
	 * 
	 * @param line the line to be written
	 * @throws IOException
	 */
	public void writeLine(String line) throws IOException {
		bWriter.write(line + '\n');
	}

	/**
	 * Initialises the file writer with the set encoding, file path and append flag
	 * 
	 * @throws FileNotFoundException
	 */
	public void init() throws FileNotFoundException {
		File file = new File(fileName);
		file.getParentFile().mkdirs();
		bWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, append), Charset.forName(encoding)));
	}

	/**
	 * Closes the file writer connection
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (bWriter != null)
			bWriter.close();
	}
}