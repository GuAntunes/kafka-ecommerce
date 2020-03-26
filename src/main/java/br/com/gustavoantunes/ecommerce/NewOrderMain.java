package br.com.gustavoantunes.ecommerce;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var produder = new KafkaProducer<String, String>(properties());
		var value = "12345,678,90";
		var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
		// Apenas o producer.send(..) envia a mensagem de forma assincrona, ou seja
		// não irá apresentar o retorno se a mensagem foi enviada com sucesso
		// o get() faz com que o metodo espere o retorno da mensagem
		produder.send(record, (data, ex) -> {
			if(ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("Sucesso enviando " + data.topic() + "particao:::" + data.partition() + "/ offset " + data.offset() + "/ timestamp" + data.timestamp());
		
		}).get();
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}
}
