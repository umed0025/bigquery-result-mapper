package jp.dkosilver.bigquery.result.mapper

import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.common.base.CaseFormat
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor

annotation class BigQueryColumn(val value: String)
class BigQueryResultMapper {

    fun <T : Any> map(from: FieldValueList, to: KClass<T>): T {
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
            val fromFieldValue = from[fromName]
            when (parameter.type.classifier) {
                // FIXME
                String::class -> fromFieldValue.nullableStringValue
                Int::class -> fromFieldValue.nullableIntValue
                Long::class -> fromFieldValue.nullableLongValue
                Double::class -> fromFieldValue.nullableDoubleValue
                Boolean::class -> fromFieldValue.nullableBooleanValue
                // TODO add local date time
                // TODO create can not find converter exception.
                else -> throw java.lang.IllegalArgumentException("not found convert type=${parameter.type}")
            }
        }
        return constructor.callBy(parameterArgs)
    }
}

val FieldValue.nullableStringValue
    get() = if (this.isNull) null else this.stringValue
val FieldValue.nullableLongValue
    get() = if (this.isNull) null else this.longValue
val FieldValue.nullableIntValue
    get() = this.nullableStringValue?.toInt()
val FieldValue.nullableDoubleValue
    get() = if (this.isNull) null else this.doubleValue
val FieldValue.nullableBooleanValue
    get() = if (this.isNull) null else this.booleanValue


