package com.automic.kafka.base;

import com.automic.kafka.constants.Constants;
import com.automic.kafka.constants.ExceptionConstants;
import com.automic.kafka.exception.AutomicException;
import com.automic.kafka.util.CommonUtil;

/**
 * This class defines the execution of any action.It provides some
 * initializations and validations on common inputs .The child actions will
 * implement its executeSpecific() method as per their own need.
 */
public abstract class AbstractKafkaAction extends AbstractAction {

	protected String brokerAddress;
	protected String sslTruststoreLocation;
	protected String sslTruststorePassword;
	protected String sslKeystoreLocation;
	protected String sslkeystorePassword;
	protected String sslPassword;
	protected boolean ssl;

	public AbstractKafkaAction() {
		addOption(Constants.BROKER_ADDRESS, true, "Broker Address");
		addOption(Constants.SSL_TRUSTSTORE_LOCATION, false, "SSL Truststore Location");
		addOption(Constants.SSL_KEYSTORE_LOCATION, false, "SSL Keystore Location");
		addOption(Constants.SSL, true, "SSL");
	}

	/**
	 * This method initializes the arguments and calls the execute method.
	 *
	 * @throws AutomicException exception while executing an action
	 */
	public final void execute() throws AutomicException {
		prepareCommonInputs();
		executeSpecific();
	}

	private void prepareCommonInputs() throws AutomicException {
		brokerAddress = getOptionValue(Constants.BROKER_ADDRESS);
		if (!CommonUtil.checkNotEmpty(brokerAddress)) {
			throw new AutomicException(
					String.format(ExceptionConstants.INVALID_INPUT_PARAMETER, "Broker Address", brokerAddress));
		}
		ssl = CommonUtil.convert2Bool(getOptionValue(Constants.SSL));
		sslTruststoreLocation = getOptionValue(Constants.SSL_TRUSTSTORE_LOCATION);
		sslKeystoreLocation = getOptionValue(Constants.SSL_KEYSTORE_LOCATION);

		sslTruststorePassword = System.getenv(Constants.SSL_TSTORE_PWD);
		sslkeystorePassword = System.getenv(Constants.SSL_KSTORE_PWD);
		sslPassword = System.getenv(Constants.SSL_PWD);
	}

	/**
	 * Method to execute the action.
	 *
	 * @throws AutomicException
	 */
	protected abstract void executeSpecific() throws AutomicException;

}
