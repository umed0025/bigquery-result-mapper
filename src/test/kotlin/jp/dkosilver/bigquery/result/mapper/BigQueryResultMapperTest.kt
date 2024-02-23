package jp.dkosilver.bigquery.result.mapper

import com.google.cloud.bigquery.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BigQueryResultMapperTest {

    @Test
    fun test() {
        // given
        // when
        val from = FieldValueList.of(
            mutableListOf(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "name1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "firstName1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "lastName1"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "email1@sample.com"),
            ),
            FieldList.of(
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("first_name", StandardSQLTypeName.STRING),
                Field.of("last_name", StandardSQLTypeName.STRING),
                Field.of("email", StandardSQLTypeName.STRING),
            )
        )
        val testTarget = BigQueryResultMapper()
        val actual = testTarget.map(from, Employee::class)
        // then
        val expected = Employee(
            1,
            "name1",
            "firstName1",
            "lastName1",
            "email1@sample.com",
        )
        assertEquals(expected, actual)
    }

    data class Employee(
        val id: Int,
        val name: String,
        val firstName: String? = null,
        val lastName: String? = null,
        val email: String? = null,
    )
}
