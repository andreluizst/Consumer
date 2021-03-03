package br.com.alst.kafka.vendaIngressos.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.alst.kafka.vendaIngressos.model.Venda;
import br.com.alst.kafka.vendaIngressos.model.VendaDeserializer;

public class consumer {

	public static void main(String[] args) {
		String topic = "topico-venda-ingressos";
		Venda venda = null;
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VendaDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-1");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		try(KafkaConsumer<String, Venda> consumer = new KafkaConsumer<>(properties)){
			consumer.subscribe(Arrays.asList(topic));
			
			while(true) {
				ConsumerRecords<String, Venda> records = consumer.poll(Duration.ofSeconds(5));
				for (ConsumerRecord<String, Venda> record : records) {
					venda = record.value();
					
					if (new Random().nextBoolean()) {
						venda.setStatus("APROVADA");
					}else {
						venda.setStatus("Reprovada");
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(venda);
				}
			}
		}
		

	}

}
