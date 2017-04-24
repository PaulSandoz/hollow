/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.tools.query;

import com.netflix.hollow.core.memory.ThreadSafeBitSet;
import com.netflix.hollow.core.read.HollowReadFieldUtils;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.core.read.engine.HollowTypeReadState;
import com.netflix.hollow.core.read.engine.object.HollowObjectTypeReadState;
import com.netflix.hollow.core.schema.HollowObjectSchema;
import com.netflix.hollow.core.schema.HollowObjectSchema.FieldType;
import com.netflix.hollow.core.schema.HollowSchema.SchemaType;
import com.netflix.hollow.core.util.SimultaneousExecutor;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class HollowFieldMatchQuery {
    
    private final HollowReadStateEngine readEngine;
    
    public HollowFieldMatchQuery(HollowReadStateEngine readEngine) {
        this.readEngine = readEngine;
    }
    
    public Map<String, BitSet> findMatchingRecords(String fieldName, String fieldValue) {
        Map<String, BitSet> matches = new HashMap<String, BitSet>();
        
        for(HollowTypeReadState typeState : readEngine.getTypeStates()) {
            
            if(typeState.getSchema().getSchemaType() == SchemaType.OBJECT) {
                HollowObjectSchema schema = (HollowObjectSchema)typeState.getSchema();
                
                for(int i=0;i<schema.numFields();i++) {
                    if(schema.getFieldName(i).equals(fieldName)) {
                        HollowObjectTypeReadState objState = (HollowObjectTypeReadState)typeState;
                        
                        BitSet typeQueryMatches = null;
                        
                        if(schema.getFieldType(i) == FieldType.REFERENCE) {
                            typeQueryMatches = attemptReferenceTraversalQuery(objState, i, fieldValue);
                        } else {
                            Object queryValue = castQueryValue(fieldValue, schema.getFieldType(i));
                            
                            if(queryValue != null) {
                                typeQueryMatches = queryBasedOnValueMatches(objState, i, queryValue);
                            }
                        }
                        
                        if(typeQueryMatches != null && typeQueryMatches.cardinality() > 0)
                            matches.put(typeState.getSchema().getName(), typeQueryMatches);
                    }
                }
            }
            
        }

        return matches;
    }
    
    private BitSet attemptReferenceTraversalQuery(HollowObjectTypeReadState typeState, int fieldIdx, String fieldValue) {
        HollowTypeReadState referencedTypeState = typeState.getSchema().getReferencedTypeState(fieldIdx);
        
        if(referencedTypeState.getSchema().getSchemaType() == SchemaType.OBJECT) {
            HollowObjectTypeReadState refObjTypeState = (HollowObjectTypeReadState)referencedTypeState;
            HollowObjectSchema refSchema = refObjTypeState.getSchema();
            
            if(refSchema.numFields() == 1) {
                if(refSchema.getFieldType(0) == FieldType.REFERENCE) {
                    BitSet refQueryMatches = attemptReferenceTraversalQuery(refObjTypeState, 0, fieldValue);
                    if(refQueryMatches != null)
                        return queryBasedOnMatchedReferences(typeState, fieldIdx, refQueryMatches);
                } else {
                    Object queryValue = castQueryValue(fieldValue, refSchema.getFieldType(0));
                    
                    if(queryValue != null) {
                        BitSet refQueryMatches = queryBasedOnValueMatches(refObjTypeState, 0, queryValue);
                        if(refQueryMatches.cardinality() > 0)
                            return queryBasedOnMatchedReferences(typeState, fieldIdx, refQueryMatches);
                    }
                }
            }
        }
        
        return null;
    }
    
    private BitSet queryBasedOnMatchedReferences(HollowObjectTypeReadState typeState, int referenceFieldPosition, BitSet matchedReferences) {
        BitSet populatedOrdinals = typeState.getPopulatedOrdinals();
        BitSet typeQueryMatches = new BitSet(populatedOrdinals.length());
      
        int ordinal = populatedOrdinals.nextSetBit(0);
        while(ordinal != -1) {
            int refOrdinal = typeState.readOrdinal(ordinal, referenceFieldPosition);
            if(refOrdinal != -1 && matchedReferences.get(refOrdinal))
                typeQueryMatches.set(ordinal);
            ordinal = populatedOrdinals.nextSetBit(ordinal+1);
        }
        return typeQueryMatches;
    }
    
    private BitSet queryBasedOnValueMatches(HollowObjectTypeReadState typeState, int fieldPosition, Object queryValue) {
        BitSet populatedOrdinals = typeState.getPopulatedOrdinals();
        BitSet typeQueryMatches = new BitSet(populatedOrdinals.length());
      
        int ordinal = populatedOrdinals.nextSetBit(0);
        while(ordinal != -1) {
            if(HollowReadFieldUtils.fieldValueEquals(typeState, ordinal, fieldPosition, queryValue))
                typeQueryMatches.set(ordinal);
            ordinal = populatedOrdinals.nextSetBit(ordinal+1);
        }
        return typeQueryMatches;
    }

    public Map<String, ThreadSafeBitSet> findMatchingRecords(String fieldName, String fieldValue, SimultaneousExecutor executor) {
        Map<String, ThreadSafeBitSet> matches = new HashMap<String, ThreadSafeBitSet>();
        
        try {
            for(HollowTypeReadState typeState : readEngine.getTypeStates()) {
                
                if(typeState.getSchema().getSchemaType() == SchemaType.OBJECT) {
                    HollowObjectSchema schema = (HollowObjectSchema)typeState.getSchema();
                    
                    for(int i=0;i<schema.numFields();i++) {
                        if(schema.getFieldName(i).equals(fieldName)) {
                            HollowObjectTypeReadState objState = (HollowObjectTypeReadState)typeState;
                            
                            ThreadSafeBitSet typeQueryMatches = null;
                            
                            if(schema.getFieldType(i) == FieldType.REFERENCE) {
                                typeQueryMatches = attemptReferenceTraversalQuery(objState, i, fieldValue, executor);
                            } else {
                                Object queryValue = castQueryValue(fieldValue, schema.getFieldType(i));
                                
                                if(queryValue != null) {
                                    typeQueryMatches = queryBasedOnValueMatches(objState, i, queryValue, executor);
                                }
                            }
                            
                            if(typeQueryMatches != null && typeQueryMatches.cardinality() > 0)
                                matches.put(typeState.getSchema().getName(), typeQueryMatches);
                        }
                    }
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        return matches;
    }
    
    private ThreadSafeBitSet attemptReferenceTraversalQuery(HollowObjectTypeReadState typeState, int fieldIdx, String fieldValue, SimultaneousExecutor executor) throws InterruptedException, ExecutionException {
        HollowTypeReadState referencedTypeState = typeState.getSchema().getReferencedTypeState(fieldIdx);
        
        if(referencedTypeState.getSchema().getSchemaType() == SchemaType.OBJECT) {
            HollowObjectTypeReadState refObjTypeState = (HollowObjectTypeReadState)referencedTypeState;
            HollowObjectSchema refSchema = refObjTypeState.getSchema();
            
            if(refSchema.numFields() == 1) {
                if(refSchema.getFieldType(0) == FieldType.REFERENCE) {
                    ThreadSafeBitSet refQueryMatches = attemptReferenceTraversalQuery(refObjTypeState, 0, fieldValue, executor);
                    if(refQueryMatches != null)
                        return queryBasedOnMatchedReferences(typeState, fieldIdx, refQueryMatches, executor);
                } else {
                    Object queryValue = castQueryValue(fieldValue, refSchema.getFieldType(0));
                    
                    if(queryValue != null) {
                        ThreadSafeBitSet refQueryMatches = queryBasedOnValueMatches(refObjTypeState, 0, queryValue, executor);
                        if(refQueryMatches.cardinality() > 0)
                            return queryBasedOnMatchedReferences(typeState, fieldIdx, refQueryMatches, executor);
                    }
                }
            }
        }
        
        return null;
    }
    
    private ThreadSafeBitSet queryBasedOnMatchedReferences(final HollowObjectTypeReadState typeState, final int referenceFieldPosition, final ThreadSafeBitSet matchedReferences, SimultaneousExecutor executor) throws InterruptedException, ExecutionException {
        final BitSet populatedOrdinals = typeState.getPopulatedOrdinals();
        final ThreadSafeBitSet typeQueryMatches = new ThreadSafeBitSet(14, populatedOrdinals.length());
      
        final AtomicInteger atomicOrdinal = new AtomicInteger(0);
        final int bitSetLength = populatedOrdinals.length();
        
        for(int i=0;i<executor.getCorePoolSize();i++) {
            executor.execute(new Runnable() {
                public void run() {
                    int ordinal = atomicOrdinal.getAndAdd(256);
                    int endChunkOrdinal = ordinal + 256;
                    while(ordinal < bitSetLength) {
                        ordinal = populatedOrdinals.nextSetBit(ordinal);
                        while(ordinal < endChunkOrdinal && ordinal != -1) {
                            int refOrdinal = typeState.readOrdinal(ordinal, referenceFieldPosition);
                            if(refOrdinal != -1 && matchedReferences.get(refOrdinal))
                                typeQueryMatches.set(ordinal);
                            ordinal = populatedOrdinals.nextSetBit(ordinal + 1);
                        }
                        ordinal = atomicOrdinal.getAndAdd(256);
                        endChunkOrdinal = ordinal + 256;
                    }
                    
                }
            });
        }
        
        executor.awaitSuccessfulCompletionOfCurrentTasks();
        
        return typeQueryMatches;
    }
    
    private ThreadSafeBitSet queryBasedOnValueMatches(final HollowObjectTypeReadState typeState, final int fieldPosition, final Object queryValue, SimultaneousExecutor executor) throws InterruptedException, ExecutionException {
        final BitSet populatedOrdinals = typeState.getPopulatedOrdinals();
        final ThreadSafeBitSet typeQueryMatches = new ThreadSafeBitSet(14, populatedOrdinals.length());

        final AtomicInteger atomicOrdinal = new AtomicInteger(0);
        final int bitSetLength = populatedOrdinals.length();

        for(int i=0;i<executor.getCorePoolSize();i++) {
            executor.execute(new Runnable() {
                public void run() {
                    int ordinal = atomicOrdinal.getAndAdd(256);
                    int endChunkOrdinal = ordinal + 256;
                    while(ordinal < bitSetLength) {
                        ordinal = populatedOrdinals.nextSetBit(ordinal);
                        while(ordinal < endChunkOrdinal && ordinal != -1) {
                            if(HollowReadFieldUtils.fieldValueEquals(typeState, ordinal, fieldPosition, queryValue))
                                typeQueryMatches.set(ordinal);
                            ordinal = populatedOrdinals.nextSetBit(ordinal + 1);
                        }
                        ordinal = atomicOrdinal.getAndAdd(256);
                        endChunkOrdinal = ordinal + 256;
                    }
                }
            });
        }

        executor.awaitSuccessfulCompletionOfCurrentTasks();

        return typeQueryMatches;
    }

    private Object castQueryValue(String fieldValue, FieldType fieldType) {
        
        try {
            switch(fieldType) {
            case BOOLEAN:
                return Boolean.valueOf(fieldValue);
            case DOUBLE:
                return Double.parseDouble(fieldValue);
            case FLOAT:
                return Float.parseFloat(fieldValue);
            case INT:
                return Integer.parseInt(fieldValue);
            case LONG:
                return Long.parseLong(fieldValue);
            case STRING:
                return fieldValue;
            default:
                return null;
            }
        } catch(Exception e) {
            return null;
        }
    }

}
