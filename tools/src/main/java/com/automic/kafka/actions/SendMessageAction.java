package com.automic.kafka.actions;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;

import com.automic.kafka.base.AbstractKafkaAction;
import com.automic.kafka.constants.Constants;
import com.automic.kafka.constants.ExceptionConstants;
import com.automic.kafka.exception.AutomicException;
import com.automic.kafka.util.CommonUtil;

/**
 * 
 * Reads the value of the JSON content by key
 * 
 * @author vijendraparmar
 *
 */
public class SendMessageAction extends AbstractKafkaAction {

	private String topic;

	private String message;

	private int retryCount;

	/**
	 * Initializes a newly created {@code GetDataAction}
	 */
	public SendMessageAction() {
		addOption(Constants.TOPIC, true, "Topic");
		addOption(Constants.MESSAGE, true, "Message");
		addOption(Constants.RETRY_COUNT, true, "Retry Count");
	}

	@Override
	protected void executeSpecific() throws AutomicException {
		prepareAndValidateInputs();

		Producer<String, String> producer = createProducer();
		TCallback callback = new TCallback();
		ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, message);
		producer.send(data, callback);

		producer.close();
	}

	private void prepareAndValidateInputs() throws AutomicException {
		topic = getOptionValue(Constants.TOPIC);
		if (!CommonUtil.checkNotEmpty(topic)) {
			throw new AutomicException(String.format(ExceptionConstants.INVALID_INPUT_PARAMETER, "Topic", topic));
		}
		message = getOptionValue(Constants.MESSAGE);
		if (!CommonUtil.checkNotEmpty(message)) {
			throw new AutomicException(String.format(ExceptionConstants.INVALID_INPUT_PARAMETER, "Message", message));
		}
		retryCount = CommonUtil.parseStringValue(getOptionValue(Constants.RETRY_COUNT), 5);

	}

	private KafkaProducer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, retryCount);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		// configure the following three settings for SSL Encryption
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
		props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
		props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);

		// configure the following three settings for SSL Authentication
		props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
		props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslkeystorePassword);
		props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslPassword);

		return new KafkaProducer<String, String>(props);
	}

	private class TCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (e != null) {
				System.out.println("Error while producing message to topic :" + recordMetadata);
				e.printStackTrace();
			} else {
				String message = String.format("sent message to topic:%s partition:%s  offset:%s",
						recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
				System.out.println(message);
			}
		}
	}
}
