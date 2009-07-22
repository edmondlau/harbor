//
//  SiteCatalog.java
//  
//
//  Created by Edmond Lau on 2/3/06.
//  Copyright 2006 __MyCompanyName__. All rights reserved.
//
package simpledb.net;

import java.net.*;
import java.io.*;
import java.util.*;

public class SiteCatalog {

	private static SiteCatalog instance = new SiteCatalog();
	
	private Map<String, List<InetSocketAddress>> dataToWorkers;
	private List<InetSocketAddress> coordinators;
	
	public static final String workerConfig = "workers.lst";
	public static final String coordConfig = "coordinators.lst";
	
	private SiteCatalog() {   
		dataToWorkers = new HashMap<String, List<InetSocketAddress>>();
		coordinators = new LinkedList<InetSocketAddress>();
		try {
			BufferedReader in = new BufferedReader(new FileReader(workerConfig));
			String str;
			System.out.println("Loading catalog ... workers");
			while ((str = in.readLine()) != null) {
				processData(str);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		try {
			BufferedReader in = new BufferedReader(new FileReader(coordConfig));
			String str;
			System.out.println("Loading catalog ... coordinators");
			while ((str = in.readLine()) != null) {
				processCoordinator(str);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}		
	}
	
	private void processData(String str) {
		if (str.charAt(0) == '#') {
			return;
		}
		System.out.println("\t" + str);
		String[] tokens = str.split("\\s");
		String filename = tokens[0];
		List<InetSocketAddress> workers = new LinkedList<InetSocketAddress>();
		for (int i = 1; i < tokens.length; i++) {
			workers.add(parse(tokens[i]));
		}
		dataToWorkers.put(filename, workers);
	}
	
	private void processCoordinator(String str) {
		if (str.charAt(0) == '#') {
			return;
		}
		System.out.println("\t" + str);
		coordinators.add(parse(str));
	}
	
	private InetSocketAddress parse(String str) {
		int colonIndex = str.indexOf(":");
		String hostname = str.substring(0, colonIndex);
		String port = str.substring(colonIndex+1);
		return new InetSocketAddress(hostname, Integer.parseInt(port));
	}
	
	public static SiteCatalog Instance() {
		return instance;
	}
	
	public synchronized List<InetSocketAddress> getSitesFor(String data) {
		if (dataToWorkers.containsKey(data))
			return dataToWorkers.get(data);
		return new LinkedList();
	}
	
	public synchronized void addLiveSiteFor(InetSocketAddress worker, String data) {
		List<InetSocketAddress> workers;
		if (dataToWorkers.containsKey(data)) {
			workers = dataToWorkers.get(data);
		} else {
			workers = new LinkedList<InetSocketAddress>();
			dataToWorkers.put(data, workers);
		}
		workers.add(worker);
	}
	
	public synchronized List<InetSocketAddress> getCoordinators() {
		return coordinators;
	}
	
	public synchronized void removeCrashedWorker(InetSocketAddress worker) {
		for (String data : dataToWorkers.keySet()) {
			List<InetSocketAddress> workers = dataToWorkers.get(data);
			if (workers.contains(worker))
				workers.remove(worker);
		}
	}
}
