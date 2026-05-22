import com.vanniktech.maven.publish.SonatypeHost

plugins {
    id("java")
    application
    id("com.vanniktech.maven.publish") version "0.28.0"
}

// Maven coordinates — io.github.* namespace is auto-verified by Sonatype for GitHub accounts.
// The Java package (com.github.gajjela521.coalescex) is independent of the Maven groupId.
group   = "io.github.gajjela521"
version = "1.2.0"

repositories {
    mavenCentral()
}

// -----------------------------------------------------------------------
// Java toolchain — no --enable-preview needed:
//   Records, pattern-matching instanceof, and Virtual Threads are all
//   standard (non-preview) features on Java 21.
//   A published library compiled with --enable-preview would force every
//   consumer to add that flag, which is unacceptable for a library release.
// -----------------------------------------------------------------------
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

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("-Xlint:all"))
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Javadoc> {
    options {
        (this as StandardJavadocDocletOptions).apply {
            encoding = "UTF-8"
            // Suppress strict doclint so Javadoc generation never blocks the release.
            addStringOption("Xdoclint:none", "-quiet")
        }
    }
}

// Allow extra JVM args to be passed from CI for pinning smoke tests:
//   ./gradlew run -Pjvmargs="-Djdk.tracePinnedThreads=full"
val jvmArgsFromCli: String? = findProperty("jvmargs") as String?
if (!jvmArgsFromCli.isNullOrBlank()) {
    tasks.withType<JavaExec> {
        jvmArgs(jvmArgsFromCli.trim().split("\\s+".toRegex()))
    }
}

// -----------------------------------------------------------------------
// Maven Central publishing
// -----------------------------------------------------------------------
mavenPublishing {
    // Publishes to the new Sonatype Central Portal (central.sonatype.com).
    // Set automaticRelease = false to inspect the staging bundle before release.
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)

    signAllPublications()

    coordinates(
        groupId    = "io.github.gajjela521",
        artifactId = "coalescex",
        version    = version.toString()
    )

    pom {
        name.set("CoalesceX")
        description.set(
            "Enterprise-grade request-collapsing gateway for Java 21+ Virtual Thread " +
            "architectures. Prevents thundering-herd distributed I/O storms by coalescing " +
            "concurrent requests without carrier-thread pinning."
        )
        url.set("https://github.com/gajjela521/CoalesceX")
        inceptionYear.set("2024")

        licenses {
            license {
                name.set("MIT License")
                url.set("https://opensource.org/licenses/MIT")
                distribution.set("repo")
            }
        }

        developers {
            developer {
                id.set("gajjela521")
                name.set("Surya Teja Gajjela")
                email.set("gajjelasuryateja2021@gmail.com")
                url.set("https://github.com/gajjela521")
            }
        }

        scm {
            url.set("https://github.com/gajjela521/CoalesceX")
            connection.set("scm:git:https://github.com/gajjela521/CoalesceX.git")
            developerConnection.set("scm:git:ssh://git@github.com/gajjela521/CoalesceX.git")
        }

        issueManagement {
            system.set("GitHub Issues")
            url.set("https://github.com/gajjela521/CoalesceX/issues")
        }
    }
}
