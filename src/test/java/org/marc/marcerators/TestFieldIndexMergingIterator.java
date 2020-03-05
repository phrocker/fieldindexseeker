package org.marc.marcerators;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

public class TestFieldIndexMergingIterator {


    public static final String NULL = "\u0000";
    public static final String DEFAULT_SHARD = "20200201_1";
    public static final String DEFAULT_DATATYPE = "dataType";

    protected SortedMap<Key, Value> generateData(Map<String,String> fieldNameAndValues, String docId){
        return generateData(DEFAULT_SHARD,fieldNameAndValues, docId,DEFAULT_DATATYPE,Collections.EMPTY_LIST,false,0);
    }

    protected SortedMap<Key, Value> generateData(final String shard, final Map<String,String> fieldNameAndValues,final String docId,
                                                 final String dataType, Collection<String> otherDataTypes, boolean randomMissingFi , int extra_fi ){
        final SortedMap<Key, Value> map = new TreeMap<>();

        fieldNameAndValues.entrySet().forEach( x -> {
            //  row            cf                 cq
            // shard datatype\u0000uid : fieldname\u0000fieldvalue
            map.put(new Key(shard, dataType + NULL + docId, x.getKey() + NULL + x.getValue()), new Value());
            //  row            cf                 cq
            // shard fi\u0000fieldname : fieldvalue\u0000datatype\u0000uid
            if( !randomMissingFi)
                map.put(new Key(shard, "fi" + NULL + x.getKey(),x.getValue() + NULL + dataType + NULL + docId), new Value() );
            IntStream.range(0,extra_fi).forEach( rng ->{
                map.put(new Key(shard, "fi" + NULL + x.getKey(),UUID.randomUUID().toString() + NULL + dataType + NULL + docId), new Value() );
            });
            for(final String otherDt : otherDataTypes){
                map.put(new Key(shard, "fi" + NULL + x.getKey(),x.getValue() + NULL + otherDt + NULL + docId), new Value() );
            }
        });


        return map;
    }

    boolean verifyDocument(final Map<String,String> fieldNameAndValues,String docId, final String json) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        Document doc = objectMapper.readValue(json,Document.class);

        return doc.docId.equals(docId) &&
                fieldNameAndValues.equals(doc.documentFields);
    }

    SortedKeyValueIterator<Key,Value> buildIterator(final SortedMap<Key,Value> map) throws IOException {
        SortedMapIterator smi = new SortedMapIterator(map);

        Map<String, String> options = new HashMap<>();

        FieldIndexMergingIterator iter = new FieldIndexMergingIterator();

        iter.init(smi, options, new MockIteratorEnvironment());

        return iter;
    }


    @Test
    public void testFindDoc() throws IOException {

        Map<String,String> fieldNameAndValues = new HashMap<>();
        fieldNameAndValues.put("FIELDA","value");
        fieldNameAndValues.put("FIELDB","value4");
        fieldNameAndValues.put("FIELDC","value3");
        fieldNameAndValues.put("FIELDD","value2");

        final String docId = UUID.randomUUID().toString();

        SortedKeyValueIterator<Key,Value> skvi = buildIterator( generateData(fieldNameAndValues,docId));

        Collection<ByteSequence> sequences = Collections.emptyList();
        Key topKey = new Key(DEFAULT_SHARD,DEFAULT_DATATYPE + NULL + docId);
        skvi.seek(new Range(topKey,true,topKey.followingKey(PartialKey.ROW_COLFAM),false), sequences, true);

        Assert.assertTrue( skvi.hasTop() );
        Assert.assertTrue(verifyDocument(fieldNameAndValues,docId,skvi.getTopValue().toString()));
    }

    @Test
    public void testExtraFiKeys() throws IOException {

        Map<String,String> fieldNameAndValues = new HashMap<>();
        fieldNameAndValues.put("FIELDA","value");
        fieldNameAndValues.put("FIELDB","value4");
        fieldNameAndValues.put("FIELDC","value3");
        fieldNameAndValues.put("FIELDD","value2");

        final String docId = UUID.randomUUID().toString();

        SortedKeyValueIterator<Key,Value> skvi = buildIterator(
                generateData(DEFAULT_SHARD /** shard **/,fieldNameAndValues,docId,"datatypey",Collections.EMPTY_LIST /** other data types */,false /** missing fis */, 20 /**
                 extra fi*/));

        Collection<ByteSequence> sequences = Collections.emptyList();
        Key topKey = new Key(DEFAULT_SHARD,"datatypey" + NULL + docId);

        skvi.seek(new Range(topKey,true,topKey.followingKey(PartialKey.ROW_COLFAM),false), sequences, true);

        Assert.assertTrue( skvi.hasTop() );
        Assert.assertTrue(verifyDocument(fieldNameAndValues,docId,skvi.getTopValue().toString()));
    }

    @Test
    public void testMissingFi() throws IOException {

        Map<String,String> fieldNameAndValues = new HashMap<>();
        fieldNameAndValues.put("FIELDA","value");
        fieldNameAndValues.put("FIELDB","value4");
        fieldNameAndValues.put("FIELDC","value3");
        fieldNameAndValues.put("FIELDD","value2");

        final String docId = UUID.randomUUID().toString();

        SortedKeyValueIterator<Key,Value> skvi = buildIterator(
                generateData(DEFAULT_SHARD /** shard **/,fieldNameAndValues,docId,"datatypey",Collections.EMPTY_LIST /** other data types */,true /** missing fis */, 20 /**
                 extra fi*/));

        Collection<ByteSequence> sequences = Collections.emptyList();
        Key topKey = new Key(DEFAULT_SHARD,"datatypey" + NULL + docId);

        skvi.seek(new Range(topKey,true,topKey.followingKey(PartialKey.ROW_COLFAM),false), sequences, true);

        Assert.assertFalse( skvi.hasTop() );
    }

    @Test
    public void testOtherDataTypes() throws IOException {

        Map<String,String> fieldNameAndValues = new HashMap<>();
        fieldNameAndValues.put("FIELDA","value");
        fieldNameAndValues.put("FIELDB","value4");
        fieldNameAndValues.put("FIELDC","value3");
        fieldNameAndValues.put("FIELDD","value2");

        final String docId = UUID.randomUUID().toString();

        List<String> otherDataTypes = new ArrayList<>();
        otherDataTypes.add("other");
        otherDataTypes.add("otherstuff");


        SortedKeyValueIterator<Key,Value> skvi = buildIterator(
                generateData(DEFAULT_SHARD /** shard **/,fieldNameAndValues,docId,"datatypey",otherDataTypes /** other data types */,false /** missing fis */, 20 /**
                 extra fi*/));

        Collection<ByteSequence> sequences = Collections.emptyList();
        Key topKey = new Key(DEFAULT_SHARD,"datatypey" + NULL + docId);

        skvi.seek(new Range(topKey,true,topKey.followingKey(PartialKey.ROW_COLFAM),false), sequences, true);

        Assert.assertTrue( skvi.hasTop() );
        Assert.assertTrue(verifyDocument(fieldNameAndValues,docId,skvi.getTopValue().toString()));
    }

    @Test
    public void testWrongDt() throws IOException {

        Map<String,String> fieldNameAndValues = new HashMap<>();
        fieldNameAndValues.put("FIELDA","value");
        fieldNameAndValues.put("FIELDB","value4");
        fieldNameAndValues.put("FIELDC","value3");
        fieldNameAndValues.put("FIELDD","value2");

        final String docId = UUID.randomUUID().toString();

        SortedKeyValueIterator<Key,Value> skvi = buildIterator(
                generateData(DEFAULT_SHARD /** shard **/,fieldNameAndValues,docId,"datatypey",Collections.EMPTY_LIST /** other data types */,false /** missing fis */, 20 /**
                 extra fi*/));

        Collection<ByteSequence> sequences = Collections.emptyList();
        Key topKey = new Key(DEFAULT_SHARD,DEFAULT_DATATYPE+ NULL + docId);

        skvi.seek(new Range(topKey,true,topKey.followingKey(PartialKey.ROW_COLFAM),false), sequences, true);

        Assert.assertFalse( skvi.hasTop() );
    }


    @Test
    public void testNoShard() throws IOException {

        Map<String,String> fieldNameAndValues = new HashMap<>();
        fieldNameAndValues.put("FIELDA","value");
        fieldNameAndValues.put("FIELDB","value4");
        fieldNameAndValues.put("FIELDC","value3");
        fieldNameAndValues.put("FIELDD","value2");

        final String docId = UUID.randomUUID().toString();

        SortedKeyValueIterator<Key,Value> skvi = buildIterator( generateData(fieldNameAndValues,docId));

        Collection<ByteSequence> sequences = Collections.emptyList();
        skvi.seek(new Range(), sequences, true);

        Assert.assertFalse( skvi.hasTop() );
    }
}

