package br.com.gustavoantunes.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) {
		var fraudDetectorService = new FraudDetectorService();

		try (var kafkaService = new KafkaService(FraudDetectorService.class.getName(), "ECOMMERCE_NEW_ORDER",
				fraudDetectorService::parse)) {
			kafkaService.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {
		System.out.println("-----------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}
		System.out.println("Order processed!");
	}
//
//	private static Properties properties() {
//		var properties = new Properties();
//		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//		// No momento em que criamos um grupo para receber a mensagem indica que o
//		// service que pertence ao grupo irá receber todas as mensagens
//		// Porém caso exista mais de um service, terá o comportamento de uma fila, no
//		// qual o kafka irá distribuir as mensagens entre os serviços
//		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getName());
//
//		// Impede que caso haja um rebalanceamento de algum tipo de conflito,
//		// pois o consumer só irá puxar uma mensagem de cada vez
//		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
//
//		return properties;
//	}

}
