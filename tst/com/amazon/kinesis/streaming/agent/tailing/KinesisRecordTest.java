/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.tailing;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.RandomUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.tailing.FileFlow;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants;
import com.amazon.kinesis.streaming.agent.tailing.KinesisFileFlow;
import com.amazon.kinesis.streaming.agent.tailing.KinesisRecord;
import com.amazon.kinesis.streaming.agent.tailing.TrackedFile;
import com.amazon.kinesis.streaming.agent.tailing.KinesisConstants.PartitionKeyOption;
import com.amazon.kinesis.streaming.agent.testing.TestUtils.TestBase;

public class KinesisRecordTest extends TestBase {
    @SuppressWarnings("rawtypes")
    private FileFlow flow;
    private TrackedFile file;
    
    @BeforeMethod
    public void setup() throws IOException {
        flow = Mockito.mock(KinesisFileFlow.class);
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.RANDOM);
        when(flow.getRecordTerminatorBytes()).thenReturn(KinesisFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR.getBytes(StandardCharsets.UTF_8));
        file = Mockito.mock(TrackedFile.class);
        when(file.getFlow()).thenReturn(flow);
    }

    @Test
    public void testStartEndOffset() {
    	KinesisRecord record = new KinesisRecord(file, 1023, new byte[100], 100);
        Assert.assertEquals(record.startOffset(), 1023);
        Assert.assertEquals(record.endOffset(), 1123);
    }

    @Test
    public void testRecordLength() {
    	KinesisRecord record = new KinesisRecord(file, 1023, new byte[200], 200);
    	String partitionKey = record.partitionKey();
        Assert.assertEquals(record.lengthWithOverhead(), 200 + partitionKey.length() + KinesisConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), 200 + partitionKey.length());
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testTruncate() throws IOException {
        byte[] data = RandomUtils.nextBytes((KinesisConstants.MAX_RECORD_SIZE_BYTES) + RandomUtils.nextInt(1, 100));
        KinesisRecord record = new KinesisRecord(file, 1023, data, data.length);
        record.truncate();
        Assert.assertEquals(record.lengthWithOverhead(), KinesisConstants.MAX_RECORD_SIZE_BYTES + KinesisConstants.PER_RECORD_OVERHEAD_BYTES);
        Assert.assertEquals(record.length(), KinesisConstants.MAX_RECORD_SIZE_BYTES);
        Assert.assertTrue(ByteBuffers.toString(record.data, StandardCharsets.UTF_8).endsWith(KinesisFileFlow.DEFAULT_TRUNCATED_RECORD_TERMINATOR));
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testGeneratePartitionKey() {
        final PartitionKeyOption partitionKeyOption = KinesisConstants.PartitionKeyOption.DETERMINISTIC;
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(partitionKeyOption);
        
        byte[] data = RandomUtils.nextBytes(200);
        KinesisRecord record = new KinesisRecord(file, 1023, data, data.length);
        Assert.assertNotNull(record.partitionKey());
        Assert.assertEquals(record.partitionKey(), record.generatePartitionKey(partitionKeyOption, null));
    }

    @Test
    public void testGeneratePartitionKeyWithCustomOption() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyIdentifier()).thenReturn("userId");

        String jsonData = "{\"userId\": \"12345\", \"name\": \"John\"},{\"userId\": \"34567\", \"name\": \"Doe\"}";
        byte[] data = jsonData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 36,34);

        KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());

        Assert.assertEquals(record.partitionKey(), "34567");
    }

    @Test
    public void testGeneratePartitionKeyWithInvalidIdentifier() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyIdentifier()).thenReturn("nonexistent");

        String jsonData = "{\"userId\": \"12345\", \"name\": \"John\"},{\"userId\": \"34567\", \"name\": \"Doe\"}";
        byte[] data = jsonData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 36,34);

        KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());

        Assert.assertNull(record.partitionKey());
        Assert.assertEquals(record.partitionKeyLength(), 0);
    }

    @Test
    public void testGeneratePartitionKeyWithInvalidJson() {
        when(((KinesisFileFlow)flow).getPartitionKeyOption()).thenReturn(KinesisConstants.PartitionKeyOption.CUSTOM);
        when(((KinesisFileFlow)flow).getPartitionKeyIdentifier()).thenReturn("userId");

        byte[] data = "invalid json".getBytes(StandardCharsets.UTF_8);
        ByteBuffer view = ByteBuffers.getPartialView(ByteBuffer.wrap(data), 0,12);

        KinesisRecord record = new KinesisRecord(file, 1023, view, view.remaining());

        Assert.assertNull(record.partitionKey());
        Assert.assertEquals(record.partitionKeyLength(), 0);
    }
}
