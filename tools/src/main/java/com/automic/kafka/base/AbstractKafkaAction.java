package com.automic.kafka.base;

import java.io.File;

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



	/**
	 * Json File Path
	 */
	protected File jsonFilePath;
	

	public AbstractKafkaAction() {
		addOption(Constants.JSON_FILE_PATH, true, "Json File Path");
	}

	/**
	 * This method initializes the arguments and calls the execute method.
	 *
	 * @throws AutomicException
	 *             exception while executing an action
	 */
	public final void execute() throws AutomicException {
		prepareCommonInputs();
		executeSpecific();
	}

	private void prepareCommonInputs() throws AutomicException {
		String temp = getOptionValue(Constants.JSON_FILE_PATH);
		if (!CommonUtil.checkNotEmpty(temp)) {
			throw new AutomicException(String.format(ExceptionConstants.INVALID_INPUT_PARAMETER, "Json File Path", temp));
		}
		jsonFilePath = new File(temp);
		CommonUtil.checkFileExists(jsonFilePath, "Json File Path");
		if (jsonFilePath.length() == 0) {
			throw new AutomicException("Provided Json content is empty");
		}

	}

	/**
	 * Method to execute the action.
	 *
	 * @throws AutomicException
	 */
	protected abstract void executeSpecific() throws AutomicException;

}
