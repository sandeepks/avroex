package avroEx;

import java.util.List;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.google.common.collect.Lists;

public class Types {
	private static final String MAX_LEN = "maxLength";
	public static final String LOGICAL_TYPE_PROP = "logicalType";
	private enum TypeInfo{
		SMALL_INT("small int"),
		TINYINT("tiny int"),
		INT("int"),
		FLOAT("float"),
		DECIMAL("decimal"),
		DOUBLE("double"),
		NUMBER("number"),
		DATE("date"),
		TIMESTAMP("timestamp"),
		CHAR("char"),
		VARCHAR("varchar");
		
		private String name;

		private TypeInfo(String name) {
			this.name = name;
		}
		
		public static TypeInfo of(String s) {
			TypeInfo returnValue = null;
			for(TypeInfo t:TypeInfo.values()) {
				if(t.name.equalsIgnoreCase(s)) {
					returnValue = t;
					break;
				}
			}
			
			return returnValue;
		}
		
	}
	
	public static Schema as(String type, int presicion, int scale, int len) {
		Schema returnValue = null;
		switch(TypeInfo.of(type)) {
		case DATE:
			returnValue = forDate();
			break;
		case DECIMAL:
			returnValue = forDecimal(presicion, scale);
			break;
		case DOUBLE:
			returnValue = forDouble();
			break;
		case FLOAT:
			break;
		case INT:
			break;
		case NUMBER:
			break;
		case SMALL_INT:
			break;
		case TIMESTAMP:
			returnValue = forTimeStamp();
			break;
		case TINYINT:
			break;
		case CHAR:
		case VARCHAR:
			returnValue = forChars(type, len);
			break;
		default:
			break;
		
		}
		
		return returnValue;
		
	}

	private static Schema forTimeStamp() {
		List<Schema> types = Lists.newLinkedList();
		types.add(Schema.create(Type.NULL));
		Schema longs = Schema.create(Type.LONG);
		LogicalTypes.timestampMillis().addToSchema(longs);
		types.add(longs);
		return Schema.createUnion(types);
	}

	private static Schema forDate() {
		List<Schema> types = Lists.newLinkedList();
		types.add(Schema.create(Type.NULL));
		Schema ints = Schema.create(Type.INT);
		LogicalTypes.date().addToSchema(ints);
		types.add(ints);
		return Schema.createUnion(types);
	}

	private static Schema forChars(String type, int len) {
		List<Schema> types = Lists.newLinkedList();
		types.add(Schema.create(Type.NULL));
		Schema string = Schema.create(Type.STRING);
		string.addProp(LOGICAL_TYPE_PROP, "type");
		string.addProp(MAX_LEN, Integer.toString(len));
		types.add(string);
		return Schema.createUnion(types);
	}

	private static Schema forDouble() {
		List<Schema> types = Lists.newLinkedList();
		types.add(Schema.create(Type.NULL));
		types.add(Schema.create(Type.DOUBLE));
		return Schema.createUnion(types);
	}

	private static Schema forDecimal(int presicion, int scale) {
		List<Schema> types = Lists.newLinkedList();
		types.add(Schema.create(Type.NULL));
		Schema bytes = Schema.create(Type.BYTES);
		LogicalTypes.decimal(presicion, scale).addToSchema(bytes);
		types.add(bytes);
		return Schema.createUnion(types);
	}

}
