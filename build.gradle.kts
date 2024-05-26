plugins {
    id("java")
}

group = "kafka.cli.examples"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(group = "org.slf4j", name = "slf4j-nop", version = "2.0.3")
    implementation(group = "org.apache.kafka", name = "kafka-clients", version = "3.7.0")
    implementation(group = "org.apache.kafka", name = "kafka-streams", version = "3.7.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}