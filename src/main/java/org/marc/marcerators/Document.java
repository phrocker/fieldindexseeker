package org.marc.marcerators;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

public class Document {
    public Map<String,String> documentFields = new HashMap<>();

    public String docId;


    @Override
    public int hashCode(){
        return new HashCodeBuilder().append(documentFields).hashCode();
    }
}
