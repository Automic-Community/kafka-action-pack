package com.automic.kafka.actions;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SslConfigs;

import com.automic.kafka.base.AbstractKafkaAction;
import com.automic.kafka.constants.Constants;
import com.automic.kafka.constants.ExceptionConstants;
import com.automic.kafka.exception.AutomicException;
import com.automic.kafka.util.CommonUtil;
import com.automic.kafka.util.ConsoleWriter;

/**
 * 
 * This action sends a message to the Kafka broker.
 * 
 * @author vijendraparmar
 *
 */
public class SendMessageAction extends AbstractKafkaAction {

	private String topic;

	private String message;

	private int retryCount;

	/**
	 * Initializes a newly created {@code SendMessageAction}
	 */
	public SendMessageAction() {
		addOption(Constants.TOPIC, true, "Topic");
		addOption(Constants.MESSAGE, true, "Message");
		addOption(Constants.RETRY_COUNT, true, "Retry Count");
	}

	@Override
	protected void executeSpecific() throws AutomicException {
		prepareAndValidateInputs();
		Producer<String, String> producer = null;
		ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, message);
		try {
			producer = createProducer();
			RecordMetadata metadata = producer.send(data).get();

			String message = String.format("Sent message to topic:%s partition:%s  offset:%s", metadata.topic(),
					metadata.partition(), metadata.offset());
			ConsoleWriter.writeln(message);
		} catch (ExecutionException | InterruptedException | KafkaException e) {
			ConsoleWriter.writeln(e);
			throw new AutomicException("Error while producing message to topic : " + topic);
		} finally {
			if (producer != null) {
				producer.close();
			}
		}
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
		props.put(ProducerConfig.BATCH_SIZE_CONFIG,
				CommonUtil.getEnvParameter(Constants.ENV_BATCH_SIZE, Constants.BATCH_SIZE));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
				CommonUtil.getEnvParameter(Constants.ENV_BUFFER_MEMORY, Constants.BUFFER_MEMORY));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		// configure the following three settings for SSL Encryption
		if (ssl) {
			props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
			if (CommonUtil.checkNotEmpty(sslKeystoreLocation)) {
				props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
				props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
			}
			// configure the following three settings for SSL Authentication
			if (CommonUtil.checkNotEmpty(sslTruststoreLocation)) {
				props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
				props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslkeystorePassword);
				props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslPassword);
			}
		}

		return new KafkaProducer<String, String>(props);
	}
}
