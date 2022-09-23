//package au.com.ing.microservice.notificationproducer.consumer;

//Copyright (c) Microsoft Corporation. All rights reserved.

//Licensed under the MIT License.

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

//import lombok.extern.slf4j.Slf4j;

//@Slf4j
public class EyConsumerThread implements Runnable {

    private static int id;
    private boolean isPrimaryKeyInUse;
    private final String topic;
    private String primarySharedAccessKey;
    private String secondarySharedAccesskey;
    private String jaasConfigValue;
    private final String jaasConfigProperty;
    private Consumer<Long, String> consumer;
    // Each consumer needs a unique client ID per thread


    public EyConsumerThread(final String topic) {
        this.topic = topic;
        this.primarySharedAccessKey = "u1kCGnfYCGSj2Tzy6bDotcV18S4ZT8UmxfGJanbMtDU=";
        this.secondarySharedAccesskey = "JWasKBRjic0GsrUM6/UYS1aKy+miXtXvAlPjI0f5+y0=";
        //this.jaasConfigValue = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\""
        //        + " password=\"Endpoint=sb://ausupsi00eaeh03.servicebus.windows.net/;SharedAccessKeyName=FuseEventNotificationListener;SharedAccessKey=_SAK_;EntityPath=fuse-event-notification\";";
        this.jaasConfigValue = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\""
                       + " password=\"Endpoint=sb://jaifirstnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=_SAK_;EntityPath=secondeventhub\";";
        
        //Endpoint=sb://jaifirstnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=u1kCGnfYCGSj2Tzy6bDotcV18S4ZT8UmxfGJanbMtDu=
        this.jaasConfigProperty = "sasl.jaas.config";
        this.isPrimaryKeyInUse = false;
    }

    public void run() throws RuntimeException {

        while (true) {
        	final Consumer<Long, String> consumer = createConsumer();
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Polling");
            try {
                while (true) {
                    final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
                    for (ConsumerRecord<Long, String> cr : consumerRecords) {
                        System.out.println(String.format("Consumer Record:(%d, %s, %d, %d)%n", cr.key(), cr.value(), cr.partition(),
                                cr.offset()));
                    }
                    consumer.commitAsync();
                }
            } catch (TopicAuthorizationException e) {
            	System.out.println("TopicAuthorizationException : " + e);
            } catch (CommitFailedException e) {
            	System.out.println("CommitFailedException: " + e);

            } finally {
            		consumer.close();
            	}            		
            }
        }

    //}

    public Consumer<Long, String> createConsumer() throws RuntimeException {
        // Create the consumer using properties.
          return  new KafkaConsumer<>(createProperties());
            // Subscribe to the topic.
            //consumer.subscribe(Collections.singletonList(topic));
           // return consumer;

        //}
    }

    private Properties createProperties() throws RuntimeException {
        try (InputStream is = getClass().getResourceAsStream("/consumer.config")) {
            final Properties properties = new Properties();
            synchronized (EyConsumerThread.class) {
                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer#" + id);
                id++;
            }
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            // Get remaining properties from config file
            properties.load(is);
            this.isPrimaryKeyInUse = !this.isPrimaryKeyInUse;
            String secretKey = this.isPrimaryKeyInUse ? this.primarySharedAccessKey : this.secondarySharedAccesskey;
            System.out.println("secret key: " + secretKey);
            properties.put(this.jaasConfigProperty, this.jaasConfigValue.replaceAll("_SAK_", secretKey));
            return  properties;
        } catch (IOException e) {
        	System.out.println("IOException: " + e);
            throw new  RuntimeException();
        }

    }

}
