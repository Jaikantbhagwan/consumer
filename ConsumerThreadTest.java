//package test;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static java.lang.Thread.currentThread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

//import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ConsumerThreadTest {
	
	private final static String TOPIC = "secondeventhub";
    private static final int PARTITION = 0;
	
	private final static int NUM_THREADS = 2;

	@Test
	public void test1() {

		TestConsumerThread exThread = Mockito.spy(new TestConsumerThread(TOPIC));

				    Thread thread = new Thread(exThread);
					MockConsumer<Long, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
			        consumer.schedulePollTask(() -> {
			            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
			            consumer.addRecord(record(TOPIC, PARTITION, 0L, "19_410_000"));
			        });
			        consumer.schedulePollTask(() -> exThread.stop1());
			        
			        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
			        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
			        startOffsets.put(tp, 0L);
			        consumer.updateBeginningOffsets(startOffsets);
					doReturn(consumer).when(exThread).createConsumer();
					thread.start();	     
				     
	}
	
/*	@Test
	public void test2() {

		TestConsumerThread exThread = Mockito.spy(new TestConsumerThread(TOPIC));

				    Thread thread = new Thread(exThread);
					MockConsumer<Long, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
			        consumer.schedulePollTask(() -> {
			            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
			            consumer.addRecord(record(TOPIC, PARTITION, 0L, "19_410_000"));
			        });
			        consumer.schedulePollTask(() -> exThread.stop1());
			        
			        consumer.schedulePollTask(() -> consumer.setException(new TopicAuthorizationException("TopicAuthorizationException exception")));
			        //consumer.schedulePollTask(() -> exThread.stop1());
			        
			        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
			        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
			        startOffsets.put(tp, 0L);
			        consumer.updateBeginningOffsets(startOffsets);
					doReturn(consumer).when(exThread).createConsumer();
					thread.start();	     
				     
	}*/

	
	@Test
	public void test3() throws Exception {
		TestConsumerThread exThread = new TestConsumerThread(TOPIC);
		 Thread t = new Thread(exThread);		 
	      t.start();
	      Thread.sleep(10000);
	      System.out.println(currentThread().getName() + " is stopping user thread");
	      exThread.stop1();		 
		
	}
	
/*	@Test
	public void test4() throws Exception {
		Properties properties = Mockito.mock(Properties.class);
		InputStreamReader inputStreamReader = Mockito.mock(InputStreamReader.class);
		doThrow(new FileNotFoundException()).when(properties).load(inputStreamReader);
		
		TestConsumerThread exThread = new TestConsumerThread(TOPIC);
		 Thread t = new Thread(exThread);
		 exThread.run();
	      t.start();
	      //Thread.sleep(10000);
	      System.out.println(currentThread().getName() + " is stopping user thread");
	     // t.stop();		 
		
	}*/
	
	@Test
	public void test5() {

		TestConsumerThread exThread = Mockito.spy(new TestConsumerThread(TOPIC));

				    Thread thread = new Thread(exThread);
					MockConsumer<Long, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
			        consumer.schedulePollTask(() -> {
			            consumer.rebalance(Collections.singletonList(new TopicPartition(TOPIC, 0)));
			            consumer.addRecord(record(TOPIC, PARTITION, 0L, "19_410_000"));
			        });
			        consumer.schedulePollTask(() -> exThread.stop1());
			        
			        consumer.schedulePollTask(() -> consumer.setException(new CommitFailedException()));
			        //consumer.schedulePollTask(() -> exThread.stop1());
			        
			        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
			        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
			        startOffsets.put(tp, 0L);
			        consumer.updateBeginningOffsets(startOffsets);
					doReturn(consumer).when(exThread).createConsumer();
					thread.start();	     
				     
	}
	
    private ConsumerRecord<Long, String> record(String topic, int partition, Long country, String population) {
        return new ConsumerRecord<>(topic, partition, 0, country, population);
    }

}
