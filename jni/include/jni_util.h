/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

#ifndef OPENSEARCH_KNN_JNI_UTIL_H
#define OPENSEARCH_KNN_JNI_UTIL_H

#include <jni.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>

namespace knn_jni {

    // Interface for making calls to JNI
    class JNIUtilInterface {
    public:
        // -------------------------- EXCEPTION HANDLING ----------------------------
        // Takes the name of a Java exception type and a message and throws the corresponding exception
        // to the JVM
        virtual void ThrowJavaException(JNIEnv* env, const char* type = "", const char* message = "") = 0;

        // Checks if an exception occurred in the JVM and if so throws a C++ exception
        // This should be called after some calls to JNI functions
        virtual void HasExceptionInStack(JNIEnv* env) = 0;

        // HasExceptionInStack with ability to specify message
        virtual void HasExceptionInStack(JNIEnv* env, const std::string& message) = 0;

        // Catches a C++ exception and throws the corresponding exception to the JVM
        virtual void CatchCppExceptionAndThrowJava(JNIEnv* env) = 0;
        // --------------------------------------------------------------------------

        // ------------------------------ JAVA FINDERS ------------------------------
        // Find a java class given a particular name
        virtual jclass FindClass(JNIEnv * env, const std::string& className) = 0;

        // Find a java method given a particular class, name and signature
        virtual jmethodID FindMethod(JNIEnv * env, jclass jClass, const std::string& methodName,
                                     const std::string& methodSignature) = 0;

        // --------------------------------------------------------------------------

        // ------------------------- JAVA TO CPP CONVERTERS -------------------------
        // Returns cpp copied string from the Java string and releases the JNI Resource
        virtual std::string ConvertJavaStringToCppString(JNIEnv * env, jstring javaString) = 0;

        // Converts a java map to a cpp unordered_map<string, jobject>
        //TODO: My concern with this function is that it will make a lot of calls between the JVM. A few options
        // to explore are:
        // 1. Passing a json string and parsing it in CPP layer
        // 2. Caching some of the method and class calls
        virtual std::unordered_map<std::string, jobject> ConvertJavaMapToCppMap(JNIEnv *env, jobject parametersJ) = 0;

        // Convert a java object to cpp string, if applicable
        virtual std::string ConvertJavaObjectToCppString(JNIEnv *env, jobject objectJ) = 0;

        // Convert a java object to a cpp integer, if applicable
        virtual int ConvertJavaObjectToCppInteger(JNIEnv *env, jobject objectJ) = 0;

        virtual std::vector<float> Convert2dJavaObjectArrayToCppFloatVector(JNIEnv *env, jobjectArray array2dJ,
                                                                            int dim) = 0;

        virtual std::vector<int64_t> ConvertJavaIntArrayToCppIntVector(JNIEnv *env, jintArray arrayJ) = 0;

        // --------------------------------------------------------------------------

        // ------------------------------ MISC HELPERS ------------------------------
        virtual int GetInnerDimensionOf2dJavaFloatArray(JNIEnv *env, jobjectArray array2dJ) = 0;

        virtual int GetJavaObjectArrayLength(JNIEnv *env, jobjectArray arrayJ) = 0;

        virtual int GetJavaIntArrayLength(JNIEnv *env, jintArray arrayJ) = 0;

        virtual int GetJavaBytesArrayLength(JNIEnv *env, jbyteArray arrayJ) = 0;

        virtual int GetJavaFloatArrayLength(JNIEnv *env, jfloatArray arrayJ) = 0;
        // --------------------------------------------------------------------------
    };

    jobject GetJObjectFromMapOrThrow(std::unordered_map<std::string, jobject> map, std::string key);

    // Class that implements JNIUtilInterface methods
    class JNIUtil: public JNIUtilInterface {
    public:
        void ThrowJavaException(JNIEnv* env, const char* type = "", const char* message = "");
        void HasExceptionInStack(JNIEnv* env);
        void HasExceptionInStack(JNIEnv* env, const std::string& message);
        void CatchCppExceptionAndThrowJava(JNIEnv* env);
        jclass FindClass(JNIEnv * env, const std::string& className);
        jmethodID FindMethod(JNIEnv * env, jclass jClass, const std::string& methodName,
                             const std::string& methodSignature);
        std::string ConvertJavaStringToCppString(JNIEnv * env, jstring javaString);
        std::unordered_map<std::string, jobject> ConvertJavaMapToCppMap(JNIEnv *env, jobject parametersJ);
        std::string ConvertJavaObjectToCppString(JNIEnv *env, jobject objectJ);
        int ConvertJavaObjectToCppInteger(JNIEnv *env, jobject objectJ);
        std::vector<float> Convert2dJavaObjectArrayToCppFloatVector(JNIEnv *env, jobjectArray array2dJ, int dim);
        std::vector<int64_t> ConvertJavaIntArrayToCppIntVector(JNIEnv *env, jintArray arrayJ);
        int GetInnerDimensionOf2dJavaFloatArray(JNIEnv *env, jobjectArray array2dJ);
        int GetJavaObjectArrayLength(JNIEnv *env, jobjectArray arrayJ);
        int GetJavaIntArrayLength(JNIEnv *env, jintArray arrayJ);
        int GetJavaBytesArrayLength(JNIEnv *env, jbyteArray arrayJ);
        int GetJavaFloatArrayLength(JNIEnv *env, jfloatArray arrayJ);
    };

    // ------------------------------- CONSTANTS --------------------------------
    extern const std::string FAISS_NAME;
    extern const std::string NMSLIB_NAME;

    extern const std::string ILLEGAL_ARGUMENT_PATH;

    extern const std::string SPACE_TYPE;
    extern const std::string METHOD;
    extern const std::string PARAMETERS;
    extern const std::string TRAINING_DATASET_SIZE_LIMIT;

    extern const std::string L2;
    extern const std::string L1;
    extern const std::string LINF;
    extern const std::string COSINESIMIL;
    extern const std::string INNER_PRODUCT;

    extern const std::string NPROBES;
    extern const std::string COARSE_QUANTIZER;
    extern const std::string EF_CONSTRUCTION;
    extern const std::string EF_SEARCH;

    // --------------------------------------------------------------------------
}

#endif //OPENSEARCH_KNN_JNI_UTIL_H
