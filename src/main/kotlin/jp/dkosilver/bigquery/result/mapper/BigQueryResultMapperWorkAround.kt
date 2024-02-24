package jp.dkosilver.bigquery.result.mapper

import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.common.base.CaseFormat
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor

@Suppress("DuplicatedCode")
class BigQueryResultMapperWorkAround {

    private val converters = mapOf(
        StringConverter.to to StringConverter,
        IntConverter.to to IntConverter,
        LongConverter.to to LongConverter,
        DoubleConverter.to to DoubleConverter,
        BooleanConverter.to to BooleanConverter,
    )

    fun <T : Any> map(fromSchema: FieldList, fromRow: FieldValueList, to: KClass<T>): T {
        val from = FieldValueList.of(fromRow, fromSchema)
        if (!to.isData) throw IllegalArgumentException("to class is not data class.")
        val constructor = to.primaryConstructor ?: throw IllegalArgumentException("not found primary constructor")
        val parameterArgs = constructor.parameters.associateWith { parameter ->
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
                        columnValue.repeatedValue.map { map(fromSchema, it.recordValue, nestedClassifier) }
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

