package datawarehouse.utils;

import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils extends org.apache.commons.lang.StringUtils {

	public static void main(String[] args) {
		
	}

	public static String unicode2String(String str, String split) {
		StringBuffer sb = new StringBuffer();
		String[] arr = str.split(split + "u");
		int len = arr.length;
		sb.append(arr[0]);
		for (int i = 1; i < len; i++) {
			String tmp = arr[i];
			char c = (char) Integer.parseInt(tmp.substring(0, 4), 16);
			sb.append(c);
			sb.append(tmp.substring(4));
		}
		return sb.toString();
	}

	public static boolean isInterger(String str) {
		try {
			Integer.valueOf(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	// 截取指定字符之间字符串
	public static String getStringBetweenKeys(String str, String first, String second) {
		return getStringBetweenKeys(str, first, second, true);
	}

	// 截取指定字符之间字符串
	public static String getStringBetweenLastKeys(String str, String first, String second) {
		return getStringBetweenKeys(str, first, second, false);
	}

	// 截取指定字符之间字符串
	private static String getStringBetweenKeys(String str, String first, String second, boolean asc) {
		if (str != null && first != null && second != null) { // 判断所有参数不为空
			// 判断第一个查找串位置
			int firstIndex = -1;
			if (asc) {
				firstIndex = str.indexOf(first);
			} else {
				firstIndex = str.lastIndexOf(first);
				if (first.equals(second)) {
					firstIndex = str.lastIndexOf(first, firstIndex - first.length());
				}
			}
			int secondIndex = str.indexOf(second, firstIndex + first.length());
			if (secondIndex == -1) {
				secondIndex = str.length();
			}
			str = str.substring(firstIndex + first.length(), secondIndex);
		}
		return str;
	}

	// 截取指定字符后的字符串
	public static String getStringAfterKey(String str, String key) {
		return getStringAfterKey(str, key, 0);
	}

	// 截取指定字符后的字符串
	protected static String getStringAfterKey(String str, String key, int fromIndex) {
		if (str != null && key != null) {
			str = str.substring(str.indexOf(key, fromIndex) + key.length());
		}
		return str;
	}

	// 截取指定最后一个字符后的字符串
	public static String getStringAfterLastKey(String str, String key) {
		if (str != null && key != null) {
			str = str.substring(str.lastIndexOf(key) + key.length());
		}
		return str;
	}

	// 截取指定字符前的字符串
	public static String getStringBeforeKey(String str, String key) {
		if (str != null && key != null && str.indexOf(key) != -1) {
			str = str.substring(0, str.indexOf(key));
		}
		return str;
	}

	public static String getStringBeforeLastKey(String str, String key) {
		if (str != null && key != null && str.lastIndexOf(key) != -1) {
			str = str.substring(0, str.indexOf(key));
		}
		return str;
	}

	/**
	 * 转化字符编码
	 * 
	 * @param str
	 *            字符串
	 * @param fromEncoding
	 *            源编码
	 * @param toEncoding
	 *            转化编码
	 * @return
	 */
	public static String convertStringEncoding(String str, String fromEncoding, String toEncoding) {
		try {
			String s = new String(str.getBytes(fromEncoding), toEncoding);
			return s;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 将字符转换为UTF-8编码
	 * 
	 * @param str
	 * @return
	 */
	public static String convertStringToUTF8(String str) {
		return convertStringEncoding(str, "ISO8859-1", "UTF-8");
	}

	/**
	 * 生成UUID
	 * 
	 * @return
	 */
	public static String getUUID() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString().replace("-", "");
	}

	/**
	 * 是否是ip
	 * 
	 * @return
	 */
	public static boolean isIp(String ip) {
		boolean flag = false;
		ip = ip.trim();
		if (ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
			String ipPart[] = ip.split("\\.");
			if (Integer.parseInt(ipPart[0]) <= 255 && Integer.parseInt(ipPart[1]) <= 255 && Integer.parseInt(ipPart[2]) <= 255 && Integer.parseInt(ipPart[3]) <= 255) {
				flag = true;
			}
		}
		return flag;
	}

	/**
	 * 是否是数字
	 * 
	 * @return
	 */
	public static boolean isNum(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		return isNum.matches();
	}

	/**
	 * 是否是字母数字或汉字
	 * 
	 * @return
	 */
	public static boolean isNormalWord(String str) {
		Pattern pattern = Pattern.compile("[A-Za-z0-9\u4E00-\u9FBF]+");
		Matcher isCN = pattern.matcher(str);
		return isCN.matches();
	}
	
	/**
	 * 是否包含中文字符<br/>
	 * 包含中文标点符号<br/>
	 * 
	 * @param str
	 * @return
	 */
	public static boolean hasChinese(String str) {
		if (str == null) {
			return false;
		}
		char[] ch = str.toCharArray();
		for (char c : ch) {
			if (isChinese(c)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 是否全是中文字符<br/>
	 * 包含中文标点符号<br/>
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isChinese(String str) {
		if (str == null) {
			return false;
		}
		char[] ch = str.toCharArray();
		for (char c : ch) {
			if (!isChinese(c)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 是否是中文字符<br/>
	 * 包含中文标点符号<br/>
	 * 
	 * @param c
	 * @return
	 */
	private static boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS) {
			return true;
		} else if (ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS) {
			return true;
		} else if (ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION) {
			return true;
		} else if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A) {
			return true;
		} else if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B) {
			return true;
		} else if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_C) {
			return true;
		} else if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_D) {
			return true;
		} else if (ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
			return true;
		} else if (ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	/**
	 * 是否包含汉字<br/>
	 * CJK统一汉字（不包含中文的，。《》（）“‘’”、！￥等符号）<br/>
	 * 
	 * @param str
	 * @return
	 */
	public static boolean hasChineseByReg(String str) {
		if (str == null) {
			return false;
		}
		Pattern pattern = Pattern.compile("[\\u4E00-\\u9FBF]+");
		return pattern.matcher(str).find();
	}

	/**
	 * 是否全是汉字<br/>
	 * CJK统一汉字（不包含中文的，。《》（）“‘’”、！￥等符号）<br/>
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isChineseByReg(String str) {
		if (str == null) {
			return false;
		}
		Pattern pattern = Pattern.compile("[\\u4E00-\\u9FBF]+");
		return pattern.matcher(str).matches();
	}
	
	/**
	 * 是否包含汉字<br/>
	 * 根据汉字编码范围进行判断<br/>
	 * CJK统一汉字（不包含中文的，。《》（）“‘’”、！￥等符号）<br/>
	 * 
	 * @param str
	 * @return
	 */
	public static boolean hasChineseByCharRange(String str) {
		if (str == null) {
			return false;
		}
		char[] ch = str.toCharArray();
		for (char c : ch) {
			if (c >= 0x4e00 && c <= 0x9fbf) {  
                return true;
            }
		}
		return false;
	}
	
	/**
	 * 是否全是汉字<br/>
	 * 根据汉字编码范围进行判断<br/>
	 * CJK统一汉字（不包含中文的，。《》（）“‘’”、！￥等符号）<br/>
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isChineseByCharRange(String str) {
		if (str == null) {
			return false;
		}
		char[] ch = str.toCharArray();
		for (char c : ch) {
			if (c < 0x4E00 || c > 0x9FBF) {
				return false;
            }
		}
		return true;
	}

	/**
	 * 是否是字母数字
	 * 
	 * @return
	 */
	public static boolean isNumAndWord(String str) {
		Pattern pattern = Pattern.compile("[A-Za-z0-9]+");
		Matcher isCN = pattern.matcher(str);
		return isCN.matches();
	}

	/**
	 * 判断是否是合法的host
	 * 
	 * @param host
	 * @return
	 */
	public static boolean isHost(String host) {
		if (isNotBlank(host)) {
			if (host.matches("([A-Za-z0-9-]+\\.)+[a-z]{1,4}")) {
				return true;
			}
		}
		return false;
	}

	public static String getRandomNumString(int length) { // length 字符串长度
		StringBuffer buffer = new StringBuffer("0123456789");
		StringBuffer sb = new StringBuffer();
		Random r = new Random();
		int range = buffer.length();
		for (int i = 0; i < length; i++) {
			sb.append(buffer.charAt(r.nextInt(range)));
		}
		return sb.toString();
	}

	public static String getRandomString(int length) { // length 字符串长度
		StringBuffer buffer = new StringBuffer("23456789abcdefghijkmnpqrstuvwxyz");
		StringBuffer sb = new StringBuffer();
		Random r = new Random();
		int range = buffer.length();
		for (int i = 0; i < length; i++) {
			sb.append(buffer.charAt(r.nextInt(range)));
		}
		return sb.toString();
	}
}
