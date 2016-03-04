package datawarehouse.io.serde;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import datawarehouse.lang.StringUtils;
 
@SerDeSpec(schemaProps={serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class SendLogSerde extends AbstractSerDe {
 
	// params
    private List<String> columnNames = null;
    private List<TypeInfo> columnTypes = null;
    private ObjectInspector objectInspector = null;
    // seperator
    public static final String COLUMN_SEP = "\n";
 
    @Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException {
        // Read Column Names
        String columnNameProp = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        if (columnNameProp != null && columnNameProp.length() > 0) {
            columnNames = Arrays.asList(columnNameProp.split(","));
        } else {
            columnNames = new ArrayList<String>();
        }
 
        // Read Column Types
        String columnTypeProp = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        // default all string
        if (columnTypeProp == null) {
            String[] types = new String[columnNames.size()];
            Arrays.fill(types, 0, types.length, serdeConstants.STRING_TYPE_NAME);
            columnTypeProp = StringUtils.join(types, ":");
        }
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProp);
 
        // Check column and types equals
        if (columnTypes.size() != columnNames.size()) {
            throw new SerDeException("len(columnNames) != len(columntTypes)");
        }
 
        // Create ObjectInspectors from the type information for each column
        List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>();
        ObjectInspector oi;
        for (int c = 0; c < columnNames.size(); c++) {
            oi = TypeInfoUtils
                    .getStandardJavaObjectInspectorFromTypeInfo(columnTypes
                            .get(c));
            columnOIs.add(oi);
        }
        objectInspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnOIs);
 
    }
 
    @Override
    public Object deserialize(Writable wr) throws SerDeException {
        // Split to kv pair
        if (wr == null)
            return null;
        Map<String, String> kvMap = new HashMap<String, String>();
        Text text = (Text) wr;
        for (String kv : text.toString().split(COLUMN_SEP)) {
        	
        	if(kv.startsWith("Src: ")){
        		String src = kv.replace("Src: ", "");
				if (src.contains(":")) {
					src = StringUtils.substringBefore(src, ":");
				}
				kvMap.put("userip", StringUtils.isIp(src)?src:"");
        	}else if (kv.startsWith("Dst: ")) {
				String dst = kv.replace("Dst: ", "");
				if (dst.contains(":")) {
					dst = StringUtils.substringBefore(dst, ":");
				}
				kvMap.put("serverip", StringUtils.isIp(dst)?dst:"");
			}else if (kv.startsWith("timestamp: ")) {
				try {
					String timeS = kv.replace("timestamp: ", "");
					long timestamp = Long.valueOf(timeS);
					if (timeS.length() == 12) {
						timestamp = timestamp * 10;
					}
					if (timeS.length() == 11) {
						timestamp = timestamp * 100;
					}
					if (timeS.length() == 10) {
						timestamp = timestamp * 1000;
					}
					if (timeS.length() == 9) {
						timestamp = timestamp * 10000;
					}
					kvMap.put("timestamp", String.valueOf(timestamp));
				} catch (NumberFormatException e) {
					kvMap.put("timestamp", String.valueOf(0));
				}

			}else if (kv.startsWith("GET ")) {
				try {
					kvMap.put("uri", kv.substring(kv.indexOf("GET ") + 4, kv.indexOf(" HTTP/1.")));
				} catch (java.lang.StringIndexOutOfBoundsException e) {
					kvMap.put("uri", "no get ,no http/1.");
				}

			}else if (kv.startsWith("Referer: ")) {
				kvMap.put("referer",kv.replace("Referer: ", ""));
			} else if (kv.startsWith("Host: ")) {
				kvMap.put("host",kv.replace("Host: ", ""));
			} else if (kv.startsWith("User-Agent: ")) {
				kvMap.put("useragent",kv.replace("User-Agent: ", ""));
			} else if (kv.startsWith("Cookie: ")) {
				kvMap.put("cookie",kv.replace("Cookie: ", ""));
			} else if (kv.startsWith("id: ")) {
				String id = kv.replace("id: ", "");
				if (id.matches("\\d+")) {
					try {
						kvMap.put("id",id);
					} catch (Exception e) {
					}
				}
			}
        }
 
        // Set according to col_names and col_types
        ArrayList<Object> row = new ArrayList<Object>();
        String colName = null;
        TypeInfo type_info = null;
        Object obj = null;
        for (int i = 0; i < columnNames.size(); i++) {
            colName = columnNames.get(i);
            type_info = columnTypes.get(i);
            obj = null;
            String value = kvMap.get(colName);
            if (type_info.getCategory() == ObjectInspector.Category.PRIMITIVE) {
                PrimitiveTypeInfo p_type_info = (PrimitiveTypeInfo) type_info;
                switch (p_type_info.getPrimitiveCategory()) {
                case STRING:
                    obj = StringUtils.defaultString(value, "null");
                    break;
                case INT:
                    obj = StringUtils.isEmpty(value)?0:Integer.parseInt(value);
                    break;
                case TIMESTAMP:
                	obj = StringUtils.isEmpty(value)?null:new Timestamp(Long.parseLong(value));
                	break;
				default:
					obj = StringUtils.defaultString(value, "null");
                    break;
                }
            }
            row.add(obj);
        }
 
        return row;
    }
 
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }
 
    @Override
    public SerDeStats getSerDeStats() {
        // Not suppourt yet
        return null;
    }
 
    @Override
    public Class<? extends Writable> getSerializedClass() {
        // Not suppourt yet
        return Text.class;
    }
 
    @Override
    public Writable serialize(Object arg0, ObjectInspector arg1)
            throws SerDeException {
        // Not suppourt yet
        return null;
    }
 
}