package com.automic.kafka.constants;

/**
 * Constants class contains all the constants.
 *
 */
public class Constants {

	public static final String ACTION = "action";
	public static final String HELP = "help";

	public static final int CONNECTION_TIMEOUT = 30000;
	public static final int READ_TIMEOUT = 60000;

	public static final int MINUS_ONE = -1;
	public static final int ZERO = 0;

	public static final String YES = "YES";
	public static final String TRUE = "TRUE";
	public static final String ONE = "1";

	public static final String BROKER_ADDRESS = "brokradd";
	public static final String SSL_TRUSTSTORE_LOCATION = "tstoreloc";
	public static final String SSL_KEYSTORE_LOCATION = "kstoreloc";
	public static final String SSL_TSTORE_PWD = "SSL_TSTORE_PWD";
	public static final String SSL_KSTORE_PWD = "SSL_KSTORE_PWD";
	public static final String SSL_PWD = "SSL_PWD";

	public static final String TOPIC = "topic";
	public static final String MESSAGE = "message";
	public static final String RETRY_COUNT = "retrycount";
	public static final String SSL = "ssl";

	private Constants() {
	}
}
