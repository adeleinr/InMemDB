package com.InMemDB;

import java.util.Scanner;

public class DBEngine{
    
    private InMemDB db;
    
    public DBEngine(){
        db = new InMemDB();
    }
    
    public void run(String ...args){
        CommandFactory cmdFactory = new CommandFactory();

        try{
            Scanner sc = new Scanner(System.in);        
            String line = null;
            
            while(sc.hasNextLine() && (line =  sc.nextLine()) != "END"){
                String[] params = line.split("\\s+");
                String command = params[0];
                String[] items = null;
                // If the command has items create a list of them
                // Some commands don't have items   
                if(params.length > 1){
                    items = new String[params.length-1];                
                    for(int i = 0, x = 1;
                            i < params.length-1 && x <= params.length-1; i++, x++){
                        items[i] = params[x];
                    }
                }   

                Command cmd = cmdFactory.getCommand(command,items);
                if(cmd != null){
	                String out = cmd.execute(db);
	                if(out != ""){
	                	System.out.println(out);
	                }
                }
             }             
            
        }catch(Exception e){
             e.printStackTrace();
        }
    }
}
