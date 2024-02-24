package jp.dkosilver.bigquery.result.mapper

import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.common.base.CaseFormat
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor

@Suppress("DuplicatedCode")
class BigQueryResultMapper {

    private val converters = mapOf(
        StringConverter.to to StringConverter,
        IntConverter.to to IntConverter,
        LongConverter.to to LongConverter,
        DoubleConverter.to to DoubleConverter,
        BooleanConverter.to to BooleanConverter,
        OffsetDateTimeConverter.to to OffsetDateTimeConverter,
    )

    fun <T : Any> map(fromSchema: FieldList, fromRow: FieldValueList, to: KClass<T>): T {
        val from = FieldValueList.of(fromRow, fromSchema)
        if (!to.isData) throw IllegalArgumentException("to class is not data class.")
        val constructor = to.primaryConstructor ?: throw IllegalArgumentException("not found primary constructor")
        val parameterArgs = constructor.parameters
            .filterNot { parameter -> parameter.findAnnotation<BigQueryMapperIgnoreField>() != null }
            .associateWith { parameter ->
                val bigQueryColumn = parameter.findAnnotation<BigQueryColumn>()
                val fromName = if (bigQueryColumn != null) {
                    bigQueryColumn.value
                } else {
                    val parameterName = parameter.name ?: throw IllegalArgumentException("not found parameter name.")
                    CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, parameterName)
                }
                val columnSchema = fromSchema[fromName].subFields
                val columnValue = from[fromName]
                val parameterClassifier = parameter.type.classifier as KClass<*>
                if (converters.containsKey(parameterClassifier)) {
                    converters[parameterClassifier]?.convert(columnValue)
                } else {
                    if (parameterClassifier == List::class) {
                        val nestedClassifier = parameter.type.arguments.first().type?.classifier as KClass<*>
                        if (columnSchema == null && converters.containsKey(nestedClassifier)) {
                            // 構造体でない場合
                            columnValue.repeatedValue.map { converters[nestedClassifier]?.convert(it) }
                        } else {
                            // 構造体の場合
                            columnValue.repeatedValue.map { map(columnSchema, it.recordValue, nestedClassifier) }
                        }
                    } else {
                        if (columnSchema == null) {
                            // 構造体でない場合
                            throw IllegalArgumentException("not found convert type=${parameter.type}")
                        } else {
                            if (columnValue.value == null) {
                                null
                            } else {
                                // 構造体の場合
                                map(columnSchema, columnValue.recordValue, parameterClassifier)
                            }
                        }
                    }
                }
            }
        return constructor.callBy(parameterArgs)
    }
}

annotation class BigQueryColumn(val value: String)
annotation class BigQueryMapperIgnoreField
abstract class Converter<T : Any>(val to: KClass<T>) {

    abstract fun convert(fromColumn: FieldValue): T?
}

object StringConverter : Converter<String>(String::class) {
    override fun convert(fromColumn: FieldValue): String? {
        return fromColumn.nullableStringValue
    }
}

object IntConverter : Converter<Int>(Int::class) {
    override fun convert(fromColumn: FieldValue): Int? {
        return fromColumn.nullableIntValue
    }
}

object LongConverter : Converter<Long>(Long::class) {
    override fun convert(fromColumn: FieldValue): Long? {
        return fromColumn.nullableLongValue
    }
}

object DoubleConverter : Converter<Double>(Double::class) {
    override fun convert(fromColumn: FieldValue): Double? {
        return fromColumn.nullableDoubleValue
    }
}

object BooleanConverter : Converter<Boolean>(Boolean::class) {
    override fun convert(fromColumn: FieldValue): Boolean? {
        return fromColumn.nullableBooleanValue
    }
}

object OffsetDateTimeConverter : Converter<OffsetDateTime>(OffsetDateTime::class) {
    override fun convert(fromColumn: FieldValue): OffsetDateTime? {
        return fromColumn.nullableOffsetDateTimeValue
    }
}


val FieldValue.nullableStringValue
    get() = if (this.isNull) null else this.stringValue
val FieldValue.nullableLongValue
    get() = if (this.isNull) null else this.longValue
val FieldValue.intValue
    get() = this.stringValue.toInt()
val FieldValue.nullableIntValue
    get() = if (this.isNull) null else this.intValue
val FieldValue.nullableDoubleValue
    get() = if (this.isNull) null else this.doubleValue
val FieldValue.nullableBooleanValue
    get() = if (this.isNull) null else this.booleanValue

val FieldValue.offsetDateTimeValue: OffsetDateTime
    get() = OffsetDateTime.ofInstant(this.timestampInstant, ZoneOffset.UTC)
val FieldValue.nullableOffsetDateTimeValue
    get() = if (this.isNull) null else this.offsetDateTimeValue
