package avroEx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

public class CompatibilityChecker {

	private static SchemaValidator FULL_VALIDATOR = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest();
	public static CompatibilityChecker FULL_CHECKER = new CompatibilityChecker(FULL_VALIDATOR);
	
	private static SchemaValidator FULL_TRANSITIVE_VALIDATOR = new SchemaValidatorBuilder().mutualReadStrategy().validateAll();
	public static CompatibilityChecker FULL_TRANSITIVE_CHECKER = new CompatibilityChecker(FULL_TRANSITIVE_VALIDATOR);

	private final SchemaValidator validator;

	private CompatibilityChecker(SchemaValidator validator) {
		this.validator = validator;
	}

	public boolean isCompatible(Schema newSchema, Schema latestSchema) {
		return isCompatible(newSchema, Collections.singletonList(latestSchema));
	}

	/**
	 * Check the compatibility between the new schema and the specified schemas
	 *
	 * @param previousSchemas
	 *            Full schema history in chronological order
	 */
	boolean isCompatible(Schema newSchema, List<Schema> previousSchemas) {
		List<Schema> previousSchemasCopy = new ArrayList<>(previousSchemas);
		try {
			// Validator checks in list order, but checks should occur in reverse
			// chronological order
			Collections.reverse(previousSchemasCopy);
			validator.validate(newSchema, previousSchemasCopy);
		} catch (SchemaValidationException e) {
			return false;
		}

		return true;
	}
	
}
