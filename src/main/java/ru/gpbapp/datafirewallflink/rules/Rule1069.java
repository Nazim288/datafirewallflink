package ru.gpbapp.datafirewallflink.rules;

import ru.gpbapp.datafirewallflink.rule.Rule;

import java.util.Map;
import java.util.regex.Pattern;

public class Rule1069 implements Rule {
    private String toStringSafe(Object o){return o==null?null:o.toString();}
    private boolean regexp_like(String s, String pat){
        if(s==null) return false;
        try{ return Pattern.compile(pat).matcher(s).find(); }catch(Exception e){return false;}
    }
    public boolean apply(Map<String,String> data){
        try{
            return regexp_like(toStringSafe(data.get("ОСНОВНЫЕ СВЕДЕНИЯ.Имя")), ".*[a-zA-Z]+.*")
                    && regexp_like(toStringSafe(data.get("ОСНОВНЫЕ СВЕДЕНИЯ.Имя")), ".*[а-яА-ЯеЁ]+.*");
        } catch(Exception e){ return false; }
    }
}

