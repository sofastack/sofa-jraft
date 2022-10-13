package com.alipay.sofa.jraft.error;

/**
 * Exception for unSupport.
 *
 * @author HH
 */
public class UnSupportException extends Exception{

	private static final long serialVersionUID = -1501160423926397729L;

	public UnSupportException(String message) {
		super(message);
	}
}
