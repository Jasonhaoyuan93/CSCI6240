package edu.neu.cs6620.hw2;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;

public class ValidatorAndPartitioner {
    Map<Character,Integer> validPrefixMap;

    public ValidatorAndPartitioner(){
        validPrefixMap = new HashMap<>();
        validPrefixMap.put('m',0);
        validPrefixMap.put('M',0);
        validPrefixMap.put('n',1);
        validPrefixMap.put('N',1);
        validPrefixMap.put('o',2);
        validPrefixMap.put('O',2);
        validPrefixMap.put('p',3);
        validPrefixMap.put('P',3);
        validPrefixMap.put('q',4);
        validPrefixMap.put('Q',4);
    }

    public boolean isValid(String word){
        return !Strings.isNullOrEmpty(word) && validPrefixMap.containsKey(word.charAt(0));
    }

    public int partition(String word){
        if(Strings.isNullOrEmpty(word)||!validPrefixMap.containsKey(word.charAt(0))){
            throw new IllegalArgumentException("Dirty Data");
        }
        return validPrefixMap.get(word.charAt(0));
    }
}
