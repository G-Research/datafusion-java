use std::error::Error;

use jni::objects::{JObject, JValue};
use jni::sys::jlong;
use jni::JNIEnv;

/// Set result by calling an ObjectResultCallback
pub fn set_callback_result<T, Err: Error>(
    env: &JNIEnv,
    callback: JObject,
    address: Result<*mut T, Err>,
) {
    match address {
        Ok(address) => set_callback_result_ok(&env, callback, address),
        Err(err) => set_callback_result_error(&env, callback, &err),
    };
}

/// Set success result by calling an ObjectResultCallback
pub fn set_callback_result_ok<T>(env: &JNIEnv, callback: JObject, address: *mut T) {
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[JValue::Void, (address as jlong).into()],
    )
    .expect("Failed to call object result callback with address");
}

/// Set error result by calling an ObjectResultCallback
pub fn set_callback_result_error<T: Error>(env: &JNIEnv, callback: JObject, error: &T) {
    let err_message = env
        .new_string(error.to_string())
        .expect("Couldn't create java string for error message");
    let address = -1 as jlong;
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[err_message.into(), address.into()],
    )
    .expect("Failed to call object result callback with error");
}

/// Set result on an ObjectResult instance
pub fn set_object_result<T, Err: Error>(
    env: &JNIEnv,
    result: JObject,
    address: Result<*mut T, Err>,
) {
    match address {
        Ok(address) => {
            env.call_method(result, "setOk", "(J)V", &[(address as jlong).into()])
                .expect("Failed to call object result setOk");
        }
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string for error message");
            env.call_method(
                result,
                "setError",
                "(Ljava/lang/String;)V",
                &[err_message.into()],
            )
            .expect("Failed to call object result setError");
        }
    }
}