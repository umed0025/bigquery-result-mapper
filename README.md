# 概要

- BigQuery Result を kotlin の data class に変換するライブラリ

# 環境

- JDK 11
- kotlin 1.9.22
- BigQuery Java Client 26.32.0

# 仕様及び、制限事項

- 変換先、data class の プライマリコンストラクタを取得して BigQuery の結果から取得し設定していく
    - 変換するデータの形は 変換先のクラス型を元に推測する

# how to use

```kotlin

// result取得
val result: TableResult = queryJob.getQueryResults()

val mapper = BigQueryResultMapper()
// スキーマ情報は別途必要なため与える
// https://stackoverflow.com/questions/48320495/bigquery-java-api-to-read-an-array-of-record-retrieving-field-value-by-name-i
val users = result.iterateAll().map { mapper.map(result.schema.fields, it, User::class) }

```
