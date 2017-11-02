package avroEx;

import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NullNode;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Schemas {
	// private String s = "abc|int|1"+System.lineSeparator() +"def|varchar|2";
	private static final Splitter NEWLINE_SPLITTER = Splitter.on("\n");
	private static final Splitter PIPE_SPLITTER = Splitter.on("|");
	private static final String DOC_STRING = "Field created Automatically";

	public Schemas() {
		super();
	}

	private Schema toAvsc(String s) {
		final Schema returnValue = Schema.createRecord("Test", "Record for Test", "org.hig", false);

		Iterable<String> split = NEWLINE_SPLITTER.omitEmptyStrings().split(s);

		// split.forEach(action);

		Iterator<String> iterator = split.iterator();

		final List<Field> fields = Lists.newLinkedList();
		while (iterator.hasNext()) {
			String line = iterator.next();

			String[] array = Iterables.toArray(PIPE_SPLITTER.split(line), String.class);
			// System.out.println(TYPE_TO_DEFAULT.get(getType(array[1], "ORACLE")));
			fields.add(
					new Field(array[0], getSchema(array[1], "ORACLE"), DOC_STRING, (JsonNode) NullNode.getInstance()));

		}
		returnValue.setFields(fields);

		return returnValue;
	}

	private Schema getSchema(String string, String vendor) {
		Schema returnFieldSchema = null;

		switch (getType(string, vendor)) {
		case ORA_BLOB:
			break;
		case ORA_CHAR:
			break;
		case ORA_CLOB:
			break;
		case ORA_INT: {
			List<Schema> types = Lists.newLinkedList();
			types.add(Schema.create(Type.NULL));
			types.add(Schema.create(Type.INT));
			returnFieldSchema = Schema.createUnion(types);
		}
			break;
		case ORA_NCHAR:
		case ORA_NVARCHAR2:
		case ORA_VARCHAR2:
		case ORA_VARCHAR: {
			List<Schema> types = Lists.newLinkedList();
			types.add(Schema.create(Type.NULL));
			types.add(Schema.create(Type.STRING));
			returnFieldSchema = Schema.createUnion(types);
		}
			break;
		case ORA_NUMBER: {
			List<Schema> types = Lists.newLinkedList();
			types.add(Schema.create(Type.NULL));
			types.add(Schema.create(Type.DOUBLE));
			returnFieldSchema = Schema.createUnion(types);
		}
			break;

		case ORA_FLOAT: {
			List<Schema> types = Lists.newLinkedList();
			types.add(Schema.create(Type.NULL));
			types.add(Schema.create(Type.FLOAT));
			returnFieldSchema = Schema.createUnion(types);
		}
		default:
			break;

		}
		return returnFieldSchema;
	}

	public enum Types {
		ORA_INT, ORA_CHAR, ORA_VARCHAR, ORA_VARCHAR2, ORA_NCHAR, ORA_NVARCHAR2, ORA_BLOB, ORA_CLOB, ORA_NUMBER, ORA_FLOAT;

		public static Types getType(String type, String vendor) {
			Types returnValue = null;
			Types[] values = Types.values();

			if (null != values && values.length > 0) {
				String substring = StringUtils.substring(vendor, 0, 3);
				for (Types t : values) {
					if (createType(substring, type).equalsIgnoreCase(t.name())) {
						returnValue = t;
						break;
					}
				}

			}
			return returnValue;
		}

		private static String createType(String vendor, String type) {
			return vendor + "_" + type;

		}
	}

	private Types getType(String type, String vendor) {
		return Types.getType(type, vendor);
	}

	public static Schema merge(Iterable<Schema> schemas) {
		Iterator<Schema> iter = schemas.iterator();
		if (!iter.hasNext()) {
			return null;
		}
		Schema result = iter.next();
		while (iter.hasNext()) {
			result = merge(result, iter.next());
		}
		return result;
	}

	public static Schema merge(Schema left, Schema right) {
		Schema merged = mergeOnly(left, right);

		return merged;
	}

	private static float SIMILARITY_THRESH = 0.3f;

	@SuppressWarnings("incomplete-switch")
	private static Schema mergeOnly(Schema left, Schema right) {
		if (Objects.equal(left, right)) {
			return left;
		}
		// handle primitive type promotion; doesn't promote integers to floats
		switch (left.getType()) {
		case INT:
			if (right.getType() == Schema.Type.LONG) {
				return right;
			}
			break;
		case LONG:
			if (right.getType() == Schema.Type.INT) {
				return left;
			}
			break;
		case FLOAT:
			if (right.getType() == Schema.Type.DOUBLE) {
				return right;
			}
			break;
		case DOUBLE:
			if (right.getType() == Schema.Type.FLOAT) {
				return left;
			}
		}

		// any other cases where the types don't match must be combined by a
		// union
		if (left.getType() != right.getType()) {
			return null;
		}

		switch (left.getType()) {
		case UNION:
			return lUnion(left, right);
		case RECORD:
			if (left.getName() == null && right.getName() == null && fieldSimilarity(left, right) < SIMILARITY_THRESH) {
				return null;
			} else if (!Objects.equal(left.getName(), right.getName())) {
				return null;
			}

			Schema combinedRecord = Schema.createRecord(coalesce(left.getName(), right.getName()),
					coalesce(left.getDoc(), right.getDoc()), coalesce(left.getNamespace(), right.getNamespace()),
					false);
			combinedRecord.setFields(mergeFields(left, right));

			return combinedRecord;

		case MAP:
			return Schema.createMap(mergeOrUnion(left.getValueType(), right.getValueType()));

		case ARRAY:
			return Schema.createArray(mergeOrUnion(left.getElementType(), right.getElementType()));

		case ENUM:
			if (!Objects.equal(left.getName(), right.getName())) {
				return null;
			}
			Set<String> symbols = Sets.newLinkedHashSet();
			symbols.addAll(left.getEnumSymbols());
			symbols.addAll(right.getEnumSymbols());
			return Schema.createEnum(left.getName(), coalesce(left.getDoc(), right.getDoc()),
					coalesce(left.getNamespace(), right.getNamespace()), ImmutableList.copyOf(symbols));

		default:
			// all primitives are handled before the switch by the equality
			// check.
			// schemas that reach this point are not primitives and also not any
			// of
			// the above known types.
			throw new UnsupportedOperationException("Unknown schema type: " + left.getType());
		}
	}

	private static Schema union(Schema left, Schema right) {
		if (left.getType() == Schema.Type.UNION) {
			if (right.getType() == Schema.Type.UNION) {
				// combine the unions by adding each type in right individually
				Schema combined = left;
				for (Schema type : right.getTypes()) {
					combined = union(combined, type);
				}
				return combined;

			} else {
				boolean notMerged = true;
				// combine a union with a non-union by checking if each type
				// will merge
				List<Schema> types = Lists.newArrayList();
				Iterator<Schema> schemas = left.getTypes().iterator();
				// try to merge each type and stop when one succeeds
				while (schemas.hasNext()) {
					Schema next = schemas.next();
					Schema merged = mergeOnly(next, right);
					if (merged != null) {
						types.add(merged);
						notMerged = false;
						break;
					} else {
						// merge didn't work, add the type
						types.add(next);
					}
				}
				// add the remaining types from the left union
				while (schemas.hasNext()) {
					types.add(schemas.next());
				}

				if (right.getFields().size() > types.size()) {
					if (notMerged) {
						types.add(right);
					}
				}

				return Schema.createUnion(types);
			}
		} else if (right.getType() == Schema.Type.UNION) {
			return union(right, left);
		}

		return Schema.createUnion(ImmutableList.of(left, right));
	}

	private static Schema lUnion(Schema left, Schema right) {
		if (left.getType() == Schema.Type.UNION) {
			if (right.getType() == Schema.Type.UNION) {
				List<Schema> leftTypes = left.getTypes();
				List<Schema> rightTypes = right.getTypes();

				if (leftTypes.size() == rightTypes.size())
					return left;

			}
		}

		return Schema.createUnion(ImmutableList.of(left, right));
	}

	private static float fieldSimilarity(Schema left, Schema right) {
		// check whether the unnamed records appear to be the same record
		Set<String> leftNames = names(left.getFields());
		Set<String> rightNames = names(right.getFields());
		int common = Sets.intersection(leftNames, rightNames).size();
		float leftRatio = ((float) common) / ((float) leftNames.size());
		float rightRatio = ((float) common) / ((float) rightNames.size());
		return hmean(leftRatio, rightRatio);
	}

	private static Set<String> names(Collection<Schema.Field> fields) {
		Set<String> names = Sets.newHashSet();
		for (Schema.Field field : fields) {
			names.add(field.name());
		}
		return names;
	}

	private static float hmean(float left, float right) {
		return (2.0f * left * right) / (left + right);
	}

	@SuppressWarnings("unchecked")
	private static <E> E coalesce(E... objects) {
		for (E object : objects) {
			if (object != null) {
				return object;
			}
		}
		return null;
	}

	private static final Schema NULL = Schema.create(Schema.Type.NULL);
	private static final NullNode NULL_DEFAULT = NullNode.getInstance();

	private static List<Schema.Field> mergeFields(Schema left, Schema right) {
		List<Schema.Field> fields = Lists.newArrayList();
		for (Schema.Field leftField : left.getFields()) {
			Schema.Field rightField = right.getField(leftField.name());
			if (rightField != null) {
				fields.add(new Schema.Field(leftField.name(), mergeOrUnion(leftField.schema(), rightField.schema()),
						coalesce(leftField.doc(), rightField.doc()),
						coalesce(leftField.defaultValue(), rightField.defaultValue())));
			} else {
				if (leftField.defaultValue() != null) {
					fields.add(copy(leftField));
				} else {
					fields.add(new Schema.Field(leftField.name(), nullableForDefault(leftField.schema()),
							leftField.doc(), NULL_DEFAULT));
				}
			}
		}

		for (Schema.Field rightField : right.getFields()) {
			if (left.getField(rightField.name()) == null) {
				if (rightField.defaultValue() != null) {
					fields.add(copy(rightField));
				} else {
					fields.add(new Schema.Field(rightField.name(), nullableForDefault(rightField.schema()),
							rightField.doc(), NULL_DEFAULT));
				}
			}
		}

		return fields;
	}

	private static Schema mergeOrUnion(Schema left, Schema right) {
		Schema merged = mergeOnly(left, right);
		if (merged != null) {
			return merged;
		}
		return union(left, right);
	}

	public static Schema.Field copy(Schema.Field field) {
		return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue());
	}

	private static Schema nullableForDefault(Schema schema) {
		if (schema.getType() == Schema.Type.NULL) {
			return schema;
		}

		if (schema.getType() != Schema.Type.UNION) {
			return Schema.createUnion(ImmutableList.of(NULL, schema));
		}

		if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
			return schema;
		}

		List<Schema> types = Lists.newArrayList();
		types.add(NULL);
		for (Schema type : schema.getTypes()) {
			if (type.getType() != Schema.Type.NULL) {
				types.add(type);
			}
		}

		return Schema.createUnion(types);
	}

	private static boolean compare(Schema left, Schema right) {
		boolean returnValue = false;

		if (Objects.equal(left.getName(), right.getName()) && Objects.equal(left.getNamespace(), right.getNamespace())
				&& Objects.equal(left.getType(), right.getType())
				&& Objects.equal(left.getFields(), right.getFields())) {
			returnValue = true;
		}
		return returnValue;
	}

	public static void main(String[] args) {
		Schemas s = new Schemas();
		Schema schema = s.toAvsc("abc|int|1\ndef|varchar|2");
		// System.out.println(schema.toString(true));

		Schema reader = merge(schema, schema);
		reader.addProp("version", IntNode.valueOf(1));
		System.out.println(reader.toString(true));

		Schema writerAdd = s.toAvsc("abc|int|1\ndef|varchar|2\nefg|varchar|2");
		// System.out.println(writerAdd.toString(true));

		Schema readerAdd = merge(writerAdd, reader);
		readerAdd.addProp("version", IntNode.valueOf(2));
		System.out.println(readerAdd.toString(true));

		Schema writerDel = s.toAvsc("abc|int|1\nefg|varchar|2");
		// System.out.println(writerDel.toString(true));

		Schema readerDel = merge(writerDel, readerAdd);
		readerDel.addProp("version", IntNode.valueOf(3));
		System.out.println(readerDel.toString(true));

		Schema writerChange = s.toAvsc("abc|float|1\nefg|varchar|2");
		// System.out.println(writerChange.toString(true));

		Schema readerChange = merge(writerChange, readerDel);
		readerChange.addProp("version", IntNode.valueOf(4));
		System.out.println(readerChange.toString(true));
		
		Schema writerChange2 = s.toAvsc("xyz|float|1\nabc|float|2\nefg|varchar|3");
		Schema readerChange2 = merge(writerChange2, readerChange);
		readerChange2.addProp("version", IntNode.valueOf(4));
		System.out.println(readerChange2.toString(true));

		SchemaPairCompatibility schemaPairCompatibility = new SchemaCompatibility.SchemaPairCompatibility(
				SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, readerChange, writerChange,
				SchemaCompatibility.READER_WRITER_COMPATIBLE_MESSAGE);
		System.out.println(schemaPairCompatibility.getDescription());

		final SchemaPairCompatibility result = checkReaderWriterCompatibility(readerChange, writerChange);
		System.out.println(result.getDescription());

		System.out.println(CompatibilityChecker.FULL_TRANSITIVE_CHECKER.isCompatible(writerChange,
				Arrays.asList(readerChange2)));

	}

}
