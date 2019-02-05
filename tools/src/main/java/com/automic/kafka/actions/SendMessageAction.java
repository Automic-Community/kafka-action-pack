package com.automic.kafka.actions;

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

	/**
	 * Path to the key to retrieve its value.
	 */
	private String path;

	private boolean fail = true;

	/**
	 * Initializes a newly created {@code GetDataAction}
	 */
	public SendMessageAction() {
		addOption(Constants.PATH, true, "JSON Path");
		addOption(Constants.FAIL, true, "Fail");
	}

	@Override
	protected void executeSpecific() throws AutomicException {
		prepareAndValidateInputs();

		

	}

	public void prepareAndValidateInputs() throws AutomicException {
		path = getOptionValue(Constants.PATH);
		if (!CommonUtil.checkNotEmpty(path)) {
			throw new AutomicException(String.format(ExceptionConstants.INVALID_INPUT_PARAMETER, "JSON Path", path));
		}

		String temp = getOptionValue(Constants.FAIL);
		if (CommonUtil.checkNotEmpty(temp)) {
			fail = CommonUtil.convert2Bool(temp);
		}
	}
}
