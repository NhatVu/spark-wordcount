package wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;


public class RunWordCount {
	private static final Logger logger = Logger.getLogger(RunWordCount.class);

	public static void main(String[] args) throws Exception {
		List<String> jarDepedencies = new ArrayList<String>();
		File[] files = new File("target/dependencies").listFiles();
		// If this pathname does not denote a directory, then listFiles() returns null.

		for (File file : files) {
			if (file.isFile()) {
				jarDepedencies.add(file.getAbsolutePath());
			}
		}
		String jarDepedenciesString = String.join(",", jarDepedencies);
		Process spark = new SparkLauncher()
				.setSparkHome("/usr/local/spark")
		        .setAppResource("target/wordcount-0.0.1-SNAPSHOT.jar")
		        .setConf("spark.jars", jarDepedenciesString)
		        .setMainClass("wordcount.WordCount")
		        .addAppArgs(args).setMaster("yarn")
		        .setDeployMode("client")
		        .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
		        .setConf(SparkLauncher.EXECUTOR_CORES, "1")
		        .setConf(SparkLauncher.EXECUTOR_MEMORY, "1g")
		        .setConf("spark.executor.instances", "2")
		        .launch();

		InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(),
		        "input");
		Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
		inputThread.start();

		InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(),
		        "error");
		Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
		errorThread.start();

		System.out.println("Waiting for finish...");
		int exitCode = spark.waitFor();
		System.out.println("Finished! Exit code:" + exitCode);
	}
}

class InputStreamReaderRunnable implements Runnable {

	private BufferedReader reader;
	private static final Logger logger = Logger.getLogger(RunWordCount.class);

	private String name;

	public InputStreamReaderRunnable(InputStream is, String name) {
		this.reader = new BufferedReader(new InputStreamReader(is));
		this.name = name;
		logger.info("InputStreamReaderRunnable:  name=" + name);
	}

	public void run() {
		System.out.println("InputStream " + name + ":");
		try {
			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				
				logger.info(line);
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
