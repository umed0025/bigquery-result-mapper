package jp.dkosilver.bigquery.result.mapper

import com.google.cloud.bigquery.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.reflect.KClass

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
// sub_field name=nested_string_value, type=STRING
// sub_field name=nested_int_value, type=INTEGER
// attribute=RECORD, value=[FieldValue{attribute=PRIMITIVE, value=nested string value 1}, FieldValue{attribute=PRIMITIVE, value=1}], value type=class com.google.cloud.bigquery.FieldValueList
// name=array_struct_string_value, type=RECORD
// sub_field name=array_struct_string_value, type=STRING
// attribute=REPEATED, value=[FieldValue{attribute=RECORD, value=[FieldValue{attribute=PRIMITIVE, value=array struct string value1}]}, FieldValue{attribute=RECORD, value=[FieldValue{attribute=PRIMITIVE, value=array struct string value2}]}], value type=class com.google.cloud.bigquery.FieldValueList
class BigQueryResultMapperWorkAroundTest {

    @Nested
    @TestInstance(PER_CLASS)
    inner class ConvertTest {
        @ParameterizedTest(name = "expected = {3}")
        @MethodSource("convertTestPattern")
        fun test(fromSchema: FieldList, fromRow: FieldValueList, to: KClass<*>, expected: Any) {
            val result = BigQueryResultMapperWorkAround().map(fromSchema, fromRow, to)
            assertEquals(expected, result)
        }

        private fun convertTestPattern() = listOf(
            // formatter:off
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.STRING)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "string value 1"))),
                StringType::class,
                StringType("string value 1"),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.STRING)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))),
                StringType::class,
                StringType(null),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.INTEGER)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Int.MAX_VALUE}"))),
                IntType::class,
                IntType(Int.MAX_VALUE),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.INTEGER)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))),
                IntType::class,
                IntType(null),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.INTEGER)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Long.MAX_VALUE}"))),
                LongType::class,
                LongType(Long.MAX_VALUE),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.INTEGER)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))),
                LongType::class,
                LongType(null),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.FLOAT)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "${Double.MAX_VALUE}"))),
                DoubleType::class,
                DoubleType(Double.MAX_VALUE),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.FLOAT)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))),
                DoubleType::class,
                DoubleType(null),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.FLOAT)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"))),
                BooleanType::class,
                BooleanType(true),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.FLOAT)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))),
                BooleanType::class,
                BooleanType(null),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.STRING)),
                FieldValueList.of(
                    mutableListOf(
                        FieldValue.of(
                            FieldValue.Attribute.REPEATED,
                            listOf(
                                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value1"),
                                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value2"),
                            ),
                        )
                    )
                ),
                ListStringType::class,
                ListStringType(listOf("value1", "value2")),
            ),
            arguments(
                FieldList.of(Field.of("value", LegacySQLTypeName.STRING)),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.REPEATED, emptyList<FieldValue>()))),
                ListStringType::class,
                ListStringType(emptyList()),
            ),
            arguments(
                FieldList.of(
                    Field.of(
                        "value", LegacySQLTypeName.RECORD,
                        Field.of("int_value", LegacySQLTypeName.INTEGER),
                        Field.of("string_value", LegacySQLTypeName.STRING)
                    ),
                ),
                FieldValueList.of(
                    mutableListOf(
                        FieldValue.of(
                            FieldValue.Attribute.RECORD, FieldValueList.of(
                                mutableListOf(
                                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                                    FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value2"),
                                )
                            )
                        )
                    )
                ),
                ChildType::class,
                ChildType(Child(1, "value2")),
            ),
            arguments(
                FieldList.of(
                    Field.of(
                        "value", LegacySQLTypeName.RECORD,
                        Field.of("int_value", LegacySQLTypeName.INTEGER),
                        Field.of("string_value", LegacySQLTypeName.STRING)
                    ),
                ),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))),
                ChildType::class,
                ChildType(null),
            ),
            arguments(
                FieldList.of(
                    Field.of(
                        "value", LegacySQLTypeName.RECORD,
                        Field.of("int_value", LegacySQLTypeName.INTEGER),
                        Field.of("string_value", LegacySQLTypeName.STRING)
                    ),
                ),
                FieldValueList.of(
                    mutableListOf(
                        FieldValue.of(
                            FieldValue.Attribute.REPEATED, listOf(
                                FieldValue.of(
                                    FieldValue.Attribute.RECORD,
                                    FieldValueList.of(
                                        mutableListOf(
                                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10"),
                                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value1-2"),
                                        )
                                    )
                                ),
                                FieldValue.of(
                                    FieldValue.Attribute.RECORD,
                                    FieldValueList.of(
                                        mutableListOf(
                                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "20"),
                                            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "value2-2"),
                                        )
                                    )
                                ),
                            )
                        )
                    )
                ),
                ListChildType::class,
                ListChildType(
                    listOf(
                        Child(10, "value1-2"),
                        Child(20, "value2-2")
                    )
                ),
            ),
            arguments(
                FieldList.of(
                    Field.of(
                        "value", LegacySQLTypeName.RECORD,
                        Field.of("int_value", LegacySQLTypeName.INTEGER),
                        Field.of("string_value", LegacySQLTypeName.STRING)
                    ),
                ),
                FieldValueList.of(mutableListOf(FieldValue.of(FieldValue.Attribute.REPEATED, emptyList<FieldValue>()))),
                ListChildType::class,
                ListChildType(emptyList()),
            ),
            // formatter:on
        )
    }

    data class StringType(val value: String?)
    data class IntType(val value: Int?)
    data class LongType(val value: Long?)
    data class DoubleType(val value: Double?)
    data class BooleanType(val value: Boolean?)
    data class ListStringType(val value: List<String>?)

    data class ChildType(val value: Child?)
    data class Child(val intValue: Int, val stringValue: String)
    data class ListChildType(val value: List<Child>)


}
