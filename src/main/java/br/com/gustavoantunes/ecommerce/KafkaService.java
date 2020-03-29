package br.com.gustavoantunes.ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaService implements Closeable{

	private final KafkaConsumer<String, String> consumer;
	private final ConsumerFunction parse;

	public KafkaService(String groupId, String topic, ConsumerFunction parse) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, String>(properties(groupId));
		consumer.subscribe(Collections.singletonList(topic));
	}

	public void run() {
		while (true) {
			var records = consumer.poll(Duration.ofMillis(100));
			if (!records.isEmpty()) {
				System.out.println("Encontrei " + records.count() + " registros");
				for (var record : records) {
					this.parse.consume(record);
				}
			}
		}
	}

	private static Properties properties(String groupId) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// No momento em que criamos um grupo para receber a mensagem indica que o
		// service que pertence ao grupo irá receber todas as mensagens
		// Porém caso exista mais de um service, terá o comportamento de uma fila, no
		// qual o kafka irá distribuir as mensagens entre os serviços
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		return properties;
	}

	@Override
	public void close(){
		this.consumer.close();
	}
}
