package com.inavitas.main;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.inavitas.kafka.clients.MessageProcessor;


public class ApplicationStarter {

	
	// Application will start from this method .taking csv path as an argument
    public static void main(String[] args) throws IOException{
    	try {
    		if (args[0] == null || args[0].trim().isEmpty()) {
    	        System.out.println("You need to specify a path!");
    	        return;
    	    } else { 
    	    	 File file = new File(args[0]);
    	         new MessageProcessor().process(new BufferedReader(new FileReader(file)));
    	    }
    	}catch(Exception e) {
    		System.out.println("Exception occured in main method of the class  Main"+e.getMessage());
    	}
    }
 
}
