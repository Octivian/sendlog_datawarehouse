package datawarehouse.udf;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import datawarehouse.utils.MD5;

public class UdfMd5 extends UDF {
	public Text evaluate(final Text s) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		if (s == null) {
			return null;
		}
		return new Text(MD5.get(s.toString()));
	}
}
