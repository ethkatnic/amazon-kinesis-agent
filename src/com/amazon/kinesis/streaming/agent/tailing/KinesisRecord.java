/*
 * Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License. 
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/asl/
 *  
 * or in the "license" file accompanying this file. 
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and limitations under the License.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KinesisRecord extends AbstractRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisRecord.class);
    private static final JsonFactory JSON_FACTORY = new ObjectMapper().getFactory();
    protected final String partitionKey;
    
    public KinesisRecord(TrackedFile file, long offset, ByteBuffer data, long originalLength) {
        super(file, offset, data, originalLength);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow)file.getFlow();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption(), flow.getPartitionKeyIdentifier());
    }

    public KinesisRecord(TrackedFile file, long offset, byte[] data, long originalLength) {
        super(file, offset, data, originalLength);
        Preconditions.checkNotNull(file);
        KinesisFileFlow flow = (KinesisFileFlow)file.getFlow();
        partitionKey = generatePartitionKey(flow.getPartitionKeyOption(), flow.getPartitionKeyIdentifier());
    }
    
    public String partitionKey() {
        return partitionKey;
    }

    @Override
    public long lengthWithOverhead() {
        return length() + KinesisConstants.PER_RECORD_OVERHEAD_BYTES;
    }
    
    @Override
    public long length() {
        return dataLength() + partitionKeyLength();
    }
    
    @Override
    protected int getMaxDataSize() {
        return KinesisConstants.MAX_RECORD_SIZE_BYTES - partitionKeyLength();
    }

    public int partitionKeyLength() {
        return partitionKey == null ? 0 : partitionKey.length();
    }
    
    @VisibleForTesting
    String generatePartitionKey(PartitionKeyOption option, String partitionKeyIdentifier) {
        Preconditions.checkNotNull(option);
        
        if (option == PartitionKeyOption.DETERMINISTIC) {
            Hasher hasher = Hashing.md5().newHasher();
            hasher.putBytes(data.array());
            return hasher.hash().toString();
        }
        if (option == PartitionKeyOption.RANDOM)
            return "" + ThreadLocalRandom.current().nextDouble(1000000);

        if (option == PartitionKeyOption.CUSTOM) {
            try (JsonParser parser = JSON_FACTORY.createParser(data.array(), (data.arrayOffset() + data.position()), data.remaining())) {
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    if (partitionKeyIdentifier.equals(parser.currentName())) {
                        parser.nextToken();
                        return parser.getText();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Failed to generate partition key", e);
                return null;
            }
        }

        return null;
    }
}
