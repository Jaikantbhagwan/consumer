

//Copyright (c) Microsoft Corporation. All rights reserved.

//Licensed under the MIT License.

import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.errors.TopicAuthorizationException;

import org.apache.kafka.common.serialization.LongDeserializer;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileReader;

import java.io.FileNotFoundException;

import java.io.IOException;

import java.util.Collections;

import java.util.Properties;

 

//import org.json.JSONException;

//import org.json.JSONObject;

 

public class TestConsumerThread implements Runnable {

 

    private boolean isPrimaryKeyInUse;

    private final String TOPIC;

    private String PRIMARY_SHARED_ACCESS_KEY;

    private String SECONDRY_SHARED_ACCESS_KEY;

    private String JAAS_CONFIG_VALUE;

    private final String JAAS_CONFIG_PROPERTY;

 

    // Each consumer needs a unique client ID per thread

    private static int id = 0;

 

    public TestConsumerThread(final String TOPIC) {

 

        this.TOPIC = TOPIC;

        this.PRIMARY_SHARED_ACCESS_KEY = "u1kCGnfYCGSj2Tzy6bDotcV18S4ZT8UmxfGJanbMtDu=";

        this.SECONDRY_SHARED_ACCESS_KEY = "JWasKBRjic0GsrUM6/UYS1aKy+miXtXvAlPjI0f5+y0=";

        this.JAAS_CONFIG_VALUE = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://jaifirstnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=_SAK_;EntityPath=consenteventhub\";";

        this.JAAS_CONFIG_PROPERTY = "sasl.jaas.config";

        this.isPrimaryKeyInUse = false;

    }

 

    public void run() {

 

        do {

 

            final Consumer<Long, String> consumer = createConsumer();

            System.out.println("Polling");

 

            try {

                while (true) {

                    final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

                    for (ConsumerRecord<Long, String> cr : consumerRecords) {

                        System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", cr.key(), cr.value(), cr.partition(),

                                cr.offset());

/*                        try {

                            JSONObject jsonObject = new JSONObject(cr.value());

                            System.out.println("Read JSON message:" + jsonObject.get("message"));

 

                        } catch (JSONException err) {

                            System.out.println(err.toString());

                        }*/

                    }

                    consumer.commitAsync();

                }

            } catch (TopicAuthorizationException e) {

                //this.isPrimaryKeyInUse = !this.isPrimaryKeyInUse;

                System.out.println("TopicAuthorizationException He he: " + e);

            } catch (CommitFailedException e) {

                System.out.println("CommitFailedException: " + e);

            } finally {

                consumer.close();

            }

        } while (true);

    }

 

    private Consumer<Long, String> createConsumer() {

        try {

            final Properties properties = new Properties();

            synchronized (TestConsumerThread.class) {

                properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer#" + id);

                id++;

            }

            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

 

            // Get remaining properties from config file

            properties.load(new FileReader("src/main/resources/consumer.config"));

 

            this.isPrimaryKeyInUse = !this.isPrimaryKeyInUse;

            String secretKey = this.isPrimaryKeyInUse ? this.PRIMARY_SHARED_ACCESS_KEY

                    : this.SECONDRY_SHARED_ACCESS_KEY;
            
            System.out.println("secret key: " + secretKey);

            

            System.out.println("The Secret in use is: "+ (this.isPrimaryKeyInUse ? "PRIMARY_SHARED_ACCESS_KEY": "SECONDRY_SHARED_ACCESS_KEY"));

            properties.put(this.JAAS_CONFIG_PROPERTY, this.JAAS_CONFIG_VALUE.replaceAll("_SAK_", secretKey));

 
            

            // Create the consumer using properties.

            final Consumer<Long, String> consumer = new KafkaConsumer<>(properties);

 

            // Subscribe to the topic.

            consumer.subscribe(Collections.singletonList(TOPIC));

            return consumer;

 

        } catch (FileNotFoundException e) {

            System.out.println("FileNotFoundException: " + e);

            System.exit(1);

            return null; // unreachable

        } catch (IOException e) {

            System.out.println("IOException: " + e);

            System.exit(1);

            return null; // unreachable

        }

    }

}
