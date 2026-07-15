plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.plugin.serialization")
}

android {
    namespace = "com.dada.p1"
    compileSdk = 37

    defaultConfig {
        applicationId = "com.dada.p1"
        minSdk = 28
        targetSdk = 30
        versionCode = 1
        versionName = "1.0"
    }

    buildFeatures {
        compose = true
    }

    // jvmTarget intentionally not set here: AGP 9.x built-in Kotlin derives it from compileOptions.targetCompatibility below
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}

dependencies {
    implementation("com.squareup.okhttp3:okhttp:4.12.0") // WebSocket transport
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3") // DTO parsing
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0") // Flow/SharedFlow/StateFlow
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.4")

    testImplementation("junit:junit:4.13.2") // JUnit4 runner for app/src/test
    testImplementation("io.mockk:mockk:1.14.11") // mocks BitgetWebSocketClient without needing it to be `open`
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0") // runTest/backgroundScope, matches coroutines-core version above
}
