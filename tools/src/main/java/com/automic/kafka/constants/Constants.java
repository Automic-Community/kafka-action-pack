package com.automic.kafka.constants;

/**
 * Constants class contains all the constants.
 *
 */
public class Constants {

	public static final String ACTION = "action";
	public static final String HELP = "help";

	public static final String ENV_CONNECTION_TIMEOUT = "ENV_CONNECTION_TIMEOUT";
	public static final String ENV_READ_TIMEOUT = "ENV_READ_TIMEOUT";
	public static final String ENV_API_VERSION = "ENV_API_VERSION";
	public static final String ENV_PASSWORD = "UC4_DECRYPTED_PWD";

	public static final String AGENT_PASSWORD = "UC4_DECRYPTED_AGENT_PWD";

	public static final int CONNECTION_TIMEOUT = 30000;
	public static final int READ_TIMEOUT = 60000;
	public static final String API_VERSION = "v4";

	public static final String BASE_URL = "baseurl";
	public static final String HTTPS = "https";
	public static final String SKIP_CERT_VALIDATION = "ssl";

	public static final int MINUS_ONE = -1;
	public static final int ZERO = 0;

	public static final String YES = "YES";
	public static final String TRUE = "TRUE";
	public static final String ONE = "1";

	public static final String PATH = "path";
	public static final String DEFAULT_VARIABLE = "UC4RB_JSN_DATA";
	public static final String JSON_FILE_PATH = "json";
	public static final String KEY = "key";
	public static final String FAIL = "fail";
	
	public static final String DOWNLOADPATH = "downloadPath";
	public static final String VALUE_EXISTS = "UC4RB_JSN_VALUE_EXISTS";
	public static final String VALUE_TYPE = "valuetype";
	public static final String OVERWRITE = "overwrite";
	public static final String VALUE = "value";

	private Constants() {
	}
}
