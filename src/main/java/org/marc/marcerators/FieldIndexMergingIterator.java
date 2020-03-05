package org.marc.marcerators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang3.StringUtils;


import java.io.IOException;
import java.util.*;

/**
 * Purpose: Using a document range will deep copy and seek to all FI keys
 * merging those sources to ensure that the document exists.
 *
 * Assumptions: Assumes that the incoming range is a document range, meaning
 *
 * Key(shard,datatype \x00 uid );
 */
public class FieldIndexMergingIterator extends WrappingIterator {


    public static final String NULL = "\u0000";
    public static final String FIELDS_TO_SKIP = "FIELDS_TO_SKIP";
    Key topKey = null;
    Value topValue = null;

    private IteratorEnvironment env = null;

    private String shard;

    private List<String> fieldsToSkip = new ArrayList<>();

    Collection<SortedKeyValueIterator<Key, Value>> deepCopiedSources = new ArrayList<>();
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // seek to the document
        super.seek(range,columnFamilies,inclusive);

        if (!range.isInfiniteStartKey()) {
            shard = range.getStartKey().getRow().toString();
            Document doc = findTop();
            if (null != doc) {
                doc = mergeSources(doc);
                if (doc != null) {
                    topValue = new Value(docToJson(doc));
                } else {
                    topValue = null;
                }
            } else {
                topValue = null;
            }
        }
    }

    @Override
    public boolean hasTop() {
       return topValue != null;
    }

    @Override
    public Key getTopKey() {
        return topValue != null ? topKey : null;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }


    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment iterEnv) throws IOException {
        super.init(source,options,iterEnv);
        env = iterEnv;

        Splitter.on(",").split( options.getOrDefault(FIELDS_TO_SKIP,"LOAD_DATE,RAW_FILE,TERM_COUNT") ).forEach(fieldsToSkip::add);
    }

    /**
     * Merge the deep copied sources to simulate field index iteration
     * @param doc document
     * @return document or null ( if sources do not merge )
     * @throws IOException Accumulo I/O Exception.
     */
    private Document mergeSources( final Document doc ) throws IOException {
        final String goalDocId = doc.docId;
        boolean notFound = false;
        for(SortedKeyValueIterator<Key, Value> source : deepCopiedSources){
            String fiUid = "";
            while(source.hasTop()){
                // look for the last null character since field values COULD have null within them
                String valueDataTypeUid = source.getTopKey().getColumnQualifier().toString();
                int lastNull = valueDataTypeUid.lastIndexOf(NULL);
                fiUid = valueDataTypeUid.substring(lastNull+1);
                if (fiUid.equals(goalDocId))
                    break;
                source.next();
            }
            if (!fiUid.equals(goalDocId))
                notFound = true;
        }
        if (notFound){
            return null;
        }
        if (StringUtils.isNotBlank(doc.docId))
            return doc;
        return null;
    }


    private String docToJson(Document doc) throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();

        final String json = objectMapper.writeValueAsString(doc);

        return json;
    }


    private Range generateRange(final String fieldName, final String fieldValue, String dataType ){
        Key topKey = new Key(shard, "fi\u0000" + fieldName, fieldValue + NULL + dataType + NULL);

        Key endKey = new Key(shard, "fi\u0000" + fieldName, fieldValue + NULL + dataType + "\uffff");

        return new Range(topKey,true,endKey,false);
    }

    private boolean notGeneratedField(final String inFieldName){
        for(String fn : fieldsToSkip){
            if ( inFieldName.equals( fn ) ){
                return false;
            }
        }
        return true;
    }

    /**
     * Find the document specified by the sought source
     * @return Document or null
     * @throws IOException I/O Exception accessing accumulo data.
     */
    private Document findTop() throws IOException {
        Document doc = null;

        if (getSource().hasTop()){
            doc = new Document();
            topKey = getSource().getTopKey();
            while(getSource().hasTop() &&
                    topKey.equals(getSource().getTopKey(), PartialKey.ROW_COLFAM)){

                // data type and UID
                final String [] dtUid = getSource().getTopKey().getColumnFamily().toString().split("\u0000");

                // field name and value
                final String [] fieldNameAndValue = getSource().getTopKey().getColumnQualifier().toString().split("\u0000");

                if (!fieldsToSkip.contains(fieldNameAndValue[0])) {
                    doc.documentFields.put( fieldNameAndValue[0], fieldNameAndValue[1] );
                    // deep copy our source and seek to the fi\x00fieldname to merge.
                    SortedKeyValueIterator<Key, Value> source = getSource().deepCopy(env);

                    doc.docId = dtUid[1];

                    source.seek(generateRange(fieldNameAndValue[0], fieldNameAndValue[1], dtUid[0]), Collections.EMPTY_LIST, false);

                    deepCopiedSources.add(source);
                }
                getSource().next();
            }
        }
        return doc;
    }

}

