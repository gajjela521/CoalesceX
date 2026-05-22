plugins {
    id("java")
    application
}

group   = "com.github.gajjela521"
version = "1.2.0"

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
    // Enable preview features for the forked run process.
    applicationDefaultJvmArgs = listOf("--enable-preview")
}

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.13")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.13")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("--enable-preview", "-Xlint:all", "-Xlint:-preview"))
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-preview")
}

// Allow extra JVM args to be passed in from CI:
//   ./gradlew run --jvm-args="-Djdk.tracePinnedThreads=full"
val jvmArgsFromCli: String? = findProperty("jvmargs") as String?
if (!jvmArgsFromCli.isNullOrBlank()) {
    tasks.withType<JavaExec> {
        jvmArgs(jvmArgsFromCli.trim().split("\\s+".toRegex()))
    }
}
