plugins {
    kotlin("jvm") version "1.9.22"
}

group = "jp.osilver"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://kotlinlang.org/docs/reflection.html
    implementation(kotlin("reflect"))
    // https://cloud.google.com/bigquery/docs/reference/libraries?hl=ja
    implementation(platform("com.google.cloud:libraries-bom:26.32.0"))
    implementation("com.google.cloud:google-cloud-bigquery")
    // https://github.com/junit-team/junit5-samples/blob/main/junit5-jupiter-starter-gradle-kotlin/build.gradle.kts
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

kotlin {
    jvmToolchain(11)
}
