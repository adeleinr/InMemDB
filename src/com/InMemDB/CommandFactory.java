package com.InMemDB;

abstract class Command{
    public String[] params;
    public Command(String params[]){
        this.params = params;
    }
    /*
     * @return A message or output
     */
    abstract public String execute(InMemDB db);
    
}

class SetCommand extends Command{
    public SetCommand(String[] params){
        super(params);
    }   
    @Override
    public String execute(InMemDB db){
    	if(this.params == null || this.params.length < 2){
    		return "SET takes 2 arguments";
    	}
        String key = params[0];
        String value = params[1];
        db.setValue(key,value);
        return ""; 
    }
}

class GetCommand extends Command{
    public GetCommand(String[] params){
        super(params);
    }
    @Override
    public String execute(InMemDB db){  
    	if(this.params == null || this.params.length != 1){
    		return "GET takes 1 argument";
    	}
        String key = params[0];
        String value = db.getValue(key);
        return (value == null ? "NULL":value);
    }
}

class UnsetCommand extends Command{
    public UnsetCommand(String[] items){
        super(items);
    }
    @Override
    public String execute(InMemDB db){
    	if(this.params == null || this.params.length != 1){
    		return "UNSET takes 1 argument";
    	}
        String key = params[0];
        db.unsetValue(key);
        return "";
    }
}

class NumEqualToCommand extends Command{
    public NumEqualToCommand(String[] params){
        super(params);
    }
    @Override
    public String execute(InMemDB db){
    	if(this.params == null || this.params.length != 1){
    		return "NUMEQUALTO takes 1 argument";
    	}
        String value = params[0];
        return db.NumEqualTo(value)+"";
    }
}

class BeginCommand extends Command{
    public BeginCommand(String[] params){
        super(params);
    }
    @Override
    public String execute(InMemDB db){  
        db.beginTransaction();
        return "";
    }
}

class RollBackCommand extends Command{
    public RollBackCommand(String[] params){
        super(params);
    }
    @Override
    public String execute(InMemDB db){  
        if(!db.rollbackTransaction()){
            return "NO TRANSACTION";
        }
        return "";
    }
}

class CommitCommand extends Command{
    public CommitCommand(String[] params){
        super(params);
    }
    @Override
    public String execute(InMemDB db){
        db.commitTransactions();
        return "";
    }
}

class EndCommand extends Command{
    public EndCommand(String[] params){
        super(params);
    }
    @Override
    public String execute(InMemDB db){        
        return "";              
    }
}

public class CommandFactory{
    
    public Command getCommand(String command, String[] params){
    	switch(command){    
            case "SET":
                return new SetCommand(params);
            case "GET":
                return new GetCommand(params);
            case "UNSET":
                return new UnsetCommand(params);
            case "NUMEQUALTO":
                return new NumEqualToCommand(params);
            case "BEGIN":
                return new BeginCommand(params);
            case "ROLLBACK":
                return new RollBackCommand(params);
            case "COMMIT":
                return new CommitCommand(params);
            case "END":
                return new EndCommand(params);
            default:
                System.out.println("Command not found");
                return null;
        }
    }    
}
