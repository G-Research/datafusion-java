use datafusion::execution::context::SessionConfig;
use jni::objects::JClass;
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_create(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let session_config = Box::new(SessionConfig::new());
    Box::into_raw(session_config) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsEnablePageIndex(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
    enabled: jboolean,
) {
    let config = unsafe { &mut *(pointer as *mut SessionConfig) };
    config.options_mut().execution.parquet.enable_page_index = enabled != 0;
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionConfig) };
}
