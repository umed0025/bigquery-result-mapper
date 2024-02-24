package jp.dkosilver.bigquery.result.mapper

import com.google.cloud.bigquery.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

class BigQueryResultMapperTest {
    // BigQuery での カラムと result のマッピング
    // name=string_value, type=STRING
    // attribute=PRIMITIVE, value=string value 11, value type=class kotlin.String
    // name=int_value, type=INTEGER
    // attribute=PRIMITIVE, value=2147483647, value type=class kotlin.String
    // name=long_value, type=INTEGER
    // attribute=PRIMITIVE, value=9223372036854775807, value type=class kotlin.String
    // name=double_value, type=FLOAT
    // attribute=PRIMITIVE, value=1.7976931348623157e+308, value type=class kotlin.String
    // name=boolean_value, type=BOOLEAN
    // attribute=PRIMITIVE, value=true, value type=class kotlin.String
    // name=timestamp_value, type=TIMESTAMP
    // attribute=PRIMITIVE, value=1708753724.0, value type=class kotlin.String
    // name=array_string_value, type=STRING
    // attribute=REPEATED, value=[FieldValue{attribute=PRIMITIVE, value=array string value1}, FieldValue{attribute=PRIMITIVE, value=array string value 2}], value type=class com.google.cloud.bigquery.FieldValueList
    // name=struct_string_string_value, type=RECORD
    // attribute=RECORD, value=[FieldValue{attribute=PRIMITIVE, value=nested string value 1}, FieldValue{attribute=PRIMITIVE, value=1}], value type=class com.google.cloud.bigquery.FieldValueList
    // name=array_struct_string_value, type=RECORD
    // attribute=REPEATED, value=[FieldValue{attribute=RECORD, value=[FieldValue{attribute=PRIMITIVE, value=array struct string value1}]}, FieldValue{attribute=RECORD, value=[FieldValue{attribute=PRIMITIVE, value=array struct string value2}]}], value type=class com.google.cloud.bigquery.FieldValueList

    @Test
    @DisplayName("プリミティブ型テスト")
    fun test() {
        // given
        // when
        val from = FieldValueList.of(
            mutableListOf(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string value 1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Int.MAX_VALUE}"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Long.MAX_VALUE}"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Double.MAX_VALUE}"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, null),
            ),
            FieldList.of(
                Field.of("string_value", LegacySQLTypeName.STRING),
                Field.of("int_value", LegacySQLTypeName.INTEGER),
                Field.of("long_value", LegacySQLTypeName.INTEGER),
                Field.of("double_value", LegacySQLTypeName.FLOAT),
                Field.of("boolean_value", LegacySQLTypeName.BOOLEAN),
                Field.of("nullable_string_value", LegacySQLTypeName.STRING),
                Field.of("nullable_int_value", LegacySQLTypeName.INTEGER),
                Field.of("nullable_long_value", LegacySQLTypeName.INTEGER),
                Field.of("nullable_double_value", LegacySQLTypeName.FLOAT),
                Field.of("nullable_boolean_value", LegacySQLTypeName.BOOLEAN),
            )
        )
        val testTarget = BigQueryResultMapper()
        val actual = testTarget.map(from, DataClassModel::class)
        // then
        val expected = DataClassModel(
            "string value 1",
            Int.MAX_VALUE,
            Long.MAX_VALUE,
            Double.MAX_VALUE,
            true,
            null,
            null,
            null,
            null,
            null,
        )
        assertEquals(expected, actual)
    }


    data class DataClassModel(
        val stringValue: String,
        val intValue: Int,
        val longValue: Long,
        val doubleValue: Double,
        val booleanValue: Boolean,
        val nullableStringValue: String?,
        val nullableIntValue: Int?,
        val nullableLongValue: Long?,
        val nullableDoubleValue: Double?,
        val nullableBooleanValue: Boolean?,
    )


}

