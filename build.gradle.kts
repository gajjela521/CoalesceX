plugins {
    id("java")
    application
}

group   = "com.github.gajjela521"
version = "1.1.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

application {
    mainClass.set("com.github.gajjela521.coalescex.SimulationHarness")
}

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    // enable virtual-thread tests on the test JVM
    jvmArgs("--enable-preview")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("--enable-preview", "-Xlint:all", "-Xlint:-preview"))
}

tasks.withType<JavaExec> {
    jvmArgs("--enable-preview")
}