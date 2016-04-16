package com.InMemDB;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class InMemDB{

	// Stores keys and values for committed data
	// or data set outside a transaction. This structure
	// is not updated unless transactions have been committed. Eg: 
	// [a] -> 10
	// [b] -> 1
    Map<String, String> data;
    
    // Inverted frequency map that holds values and how
    // many keys have that value. This structure is updated even
    // if transactions are not committed and then rolledback if needed.
    // This was an important design decision since it looks inconsistent
    // to have main (data) not modified but its freq map yes. However
    // it was needed since at any point in time we need to be able to retrieve
    // a count even though the changes might be spread across different
    // transactions. And having yet another separate global freq map seemed 
    // redundant. If a freq came down to 0 we remove the key from here. Eg:
    // [10] -> 1
    // [1]  -> 1
    Map<String, Integer> dataFreq;
    
    // Stores a history of values we have assigned per key
    // while inside any transaction block that is not committed yet.
    // Only exists if there are any open transactions.
    // This structure is key to finding any value at any point
    // in time in O(1), as opposed to having to traverse through
    // all the transaction blocks to find if it has modified this key and
    // thus contains the latest updated value.
    // This is used to know at all times, within transaction 
    // blocks, what is the current value of the key, whether it 
    // was updated by a parent transaction or by the current transaction
    // block. It does not keep track of values saved outside of transactions.
    // The values stack of a key could contain null values
    // if UNSET was performed. Eg:
    // SET a 10, BEGIN SET b = 15, SET a = 11, UNSET b, SET b 30
    // [a] -> [11]
    // [b] -> [15, null, 30]
    Map<String, Stack<String>> dataHistory;
    
    // List of maps(one per transaction block) that store the key, value that
    // have been changed within this transaction.
    // An UNSET is treated as a SET (your key) NULL, which is different than
    // in the main (data) structure that does not store null values. This is so
    // that we can know later what keys need to be rolledback. Eg:
    // trans 0
    // 	[b] -> 30
    // 	[a] -> 11
    // trans 1
    //  [b] -> 12
    Stack<Map<String,String>> transactionsData;  
    
    // List of inverted frequency maps (one per transaction block) that keep
    // the deltas in freq performed within a transaction. It does not reflect
    // any global frequency.
    // It is used for rolling back transactions. Note that the freq could be
    // negative. Eg:
    // trans 0
	    // [10] -> -1 (This means that as a side effect of a SET or UNSET in this
	    //             transaction block we had to decrease the count of 10)
	    // [11] -> 9
    // trans 1
    	// [10] -> 1
        // [4] -> -1
    Stack<Map<String, Integer>> transactionsFreq;
    
    boolean debug = false;
        
    public InMemDB(){
        data = new HashMap<String, String>();
        dataFreq = new  HashMap<String, Integer>();
    }
    /* Returns whether or not there are any open transactions
     * 
     * @return True if there are open transactions
     */
    private boolean inTransaction(){
        return (transactionsData != null && !transactionsData.isEmpty());
    }
    

    /*
     * Saves new value of a key to history. The new value could be null if
     * an UNSET is performed.
     * 
     * @param key key to store
     * @param value value to store
     */
    private void saveToHistory(String key, String value){
    	Stack<String> states = dataHistory.get(key);
    	if(states == null){
    		states = new Stack<String>();
    	}
    	states.add(value);
    	dataHistory.put(key, states);
    }
    
    /*
     * Get the latest value of a key, whether it is from main (data)
     * or stored in the history of changes done by transactions
     * 
     * @param key key we want the value of
     * @returns the latest value
     */
    private String getLatestValue(String key){
    	if(inTransaction() && dataHistory != null){
	    	Stack<String> valueStates = dataHistory.get(key);
	    	if(valueStates != null){
	    		return valueStates.peek();
	    	}else{
	    		return data.get(key);
	    	}
    	}else{
    		return data.get(key);
    	}
    }

    /*
     * Sets the value of a key, either to null if UNSET or to some other
     * value if SET.
     * The main idea when we need to set is:
     * If no transactions:
     * 	Update main data map
     *  Update main freq map for old value and for new value
     * else if transactions:
	 * 	update the local (per trasanction) data map
     *  update the local freq map
     *  update the main freq map
     *  then add new value to history
     *  This is an O(1) operation
     *  
     *  @param key key to be stored, cannot be null
     *  @param value value to be stored, can be null
     */    
    public void setValue(String key, String value){
    	Map<String, String> thisData;
    	String latestValue = getLatestValue(key);
    	if(inTransaction()){
    		thisData = transactionsData.peek();
    	}else{
    		thisData = data;
    	}
        String localValue = thisData.get(key);
        
        // Same var with same val, it is a noop
        if( localValue != null && localValue.equals(value)) return;
        // We are unsetting a var thats already been unset
        if( localValue == null && thisData.containsKey(key)
           && value == null) return;
               
        if(value == null){// We want to unset
            // Decrement count for old value
            // Key is not in local map
            if(!thisData.containsKey(key) && latestValue != null){           	
            	// Decrement count we found in history
            	decFreq(latestValue);               	        	
            }else if(thisData.containsKey(key)){ 
            	// Decrement count we had stored in local trans block
            	decFreq(localValue);
            }
                   	
            // Store value in main data map or trans data
            thisData.put(key, value);

        }else{ // We want to set

            incFreq(value);
            
            // Decrement count for old value
            // Key is not in local map
            if(!thisData.containsKey(key) && latestValue != null){           	
            	// Decrement count we found in history
            	decFreq(latestValue);              	        	
            }else if(thisData.containsKey(key)){
            	// Decrement count we had stored in local trans block
            	decFreq(localValue);
            }

            // Store value in main mem or trans data
            thisData.put(key, value);
        }
        
        if(inTransaction()){
        	saveToHistory(key, value); 
        }        
    }
    
    /*
     * Increase the frequency in all the pertinent freq maps
     * If there are transactions we update the local freq map
     * and the main freq map, else just the main freq map.
     * 
     * @param value value to be incremented
     */
    private void incFreq(String value){
    	if(inTransaction()){
	    	Integer count1 = dataFreq.get(value);
	        if(count1 == null){count1 = new Integer(0);}
	        count1++;
	        dataFreq.put(value, count1);
	        
    		Map<String, Integer> localDataFreq = transactionsFreq.peek();
	        Integer count2 = localDataFreq.get(value);
	        if(count2 == null){
	        	count2 = new Integer(0);
	        }	        
	        count2++;
	        localDataFreq.put(value, count2);
	        
	        
    	}else{
    		Integer count = dataFreq.get(value);
	        if(count == null){
	        	count = new Integer(0);
	        }
	        count++;
	        dataFreq.put(value, count);
    	}
    }
    
    /*
     * Decrease the frequency in all the pertinent freq maps
     * If there are transactions we update the local freq map
     * and the main freq map, else just the main freq map. Note that
     *  
     * @param value value to be decremented
     */
    private void decFreq(String value){
    	if(inTransaction()){
	        // Global scope
      		Integer count1 = dataFreq.get(value);
	        if(count1 == 1){
	        	dataFreq.remove(value);
	        }else{
	        	count1--;
		        dataFreq.put(value, count1);
	        }
	        // Local trans scope
	  		Map<String, Integer> localDataFreq = transactionsFreq.peek();
	        if(localDataFreq.containsKey(value)){
		        Integer count2 = localDataFreq.get(value);
		        if(count2 == 1){
		        	localDataFreq.remove(value);
		        }else{
		        	count2--;
			        localDataFreq.put(value, count2);
		        }
	        }else{
	        	// Val is not in the local freq map but we still need to record
	        	// that we decreased the count of this val
	        	localDataFreq.put(value, new Integer(-1));
	        }
    	}else if(dataFreq.containsKey(value)){
    		// Global scope
		    Integer count2 = dataFreq.get(value);
	        if(count2 == 1){
	        	dataFreq.remove(value);
	        }else{
	        	count2--;
	        	dataFreq.put(value, count2);
	        }
    	}
    }
    
    /*
     * Gets the value for a key, it will search history or main committed data
     * This is an O(1) operation
     * 
     * @return value value for this key or null
     * @param key key we want the value for
     */
    public String getValue(String key){        
        return getLatestValue(key);
    }
    
    /*
     * Unsets an existing value or does nothing otherwise
     * This is treated as a setValue with a null value
     * This is an O(1) operation.
     * @param key key we want to unset
     */
    public void unsetValue(String key){
    	setValue(key, null);        
    }
    
    /*
     * Gets the frequency of this value
     * 
     * @return numbers of keys that have the is value stored. If not key has
     * 		   this value returns 0.
     * @param value value we want to know the freq of.
     */
    public int NumEqualTo(String value){
        Integer count = dataFreq.get(value);
        if(count == null){
            count = new Integer(0);
        }
        return count;
    }
    
    /* 
     * Marks a new transaction block
     */
    public void beginTransaction(){
    	if( dataHistory == null ){
    		dataHistory = new HashMap<String, Stack<String>>();
    	}
    	if(transactionsData == null){
    		transactionsData = new Stack<Map<String, String>>();
    	}
    	if(transactionsFreq == null){
    		transactionsFreq = new Stack<Map<String, Integer>>();
    	}    		
    	transactionsData.add(new HashMap<String, String>());
        transactionsFreq.add( new HashMap<String, Integer>());
    }
    
    /*
     * Rollsback the most recent transaction block. This is the most expensive
     * operation in the DB O(n).
     * It uses the local data map to know what keys need rolling back from
     * history. No need to roll back main data memory since we never changed it.
     * It also uses the local freq map to know how much to decrease/increase
     * in the main freq map. Recall that the local per transaction freq map
     * store deltas and that the main freq map did get changed inside
     * transactions.
     */
    public boolean rollbackTransaction(){
        if(!inTransaction()){
        	return false;
        }
        // Remove values changed in this transaction from the history
        // and if history ends up empty for this key then remove key
        Map<String, String> thisTransData = transactionsData.pop();
        for(String key: thisTransData.keySet()){
        	Stack<String> values = dataHistory.get(key);	        	
        	values.pop();
        	if(values.isEmpty()){
        		dataHistory.remove(key);
        	}
        }
        // Substract the current freq in the main freq map
        Map<String, Integer> thisTransFreq = transactionsFreq.pop();      
        for(Map.Entry<String, Integer> entry: thisTransFreq.entrySet()){
        	Integer val = dataFreq.get(entry.getKey());
        	int newCount = 0;
        	if(val == null){
        		newCount = -entry.getValue();
        	}else{
        		newCount = val - entry.getValue();
        	}
        	dataFreq.put(entry.getKey(), newCount);
        }
	        
        return true;
    }
    
    /*
     * Commits to main mem by looking at the latest values in the history.
     * O(n)
     */
    public void commitTransactions(){
    	for(Map.Entry<String, Stack<String>> entry: dataHistory.entrySet()){
    		data.put(entry.getKey(), entry.getValue().pop());
    	}
    	transactionsData = null;
    	transactionsFreq = null;
        dataHistory = null;
    }
    
    /*
     * Commits all left over transactions if Commit hasn't been executed.
     */
    public void end(){
        if(!transactionsData.isEmpty()){
            commitTransactions();
        }
    } 
}


