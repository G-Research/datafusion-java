use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::file_format::FileFormat;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ArrowFormat_create(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    // Return as an Arc<dyn FileFormat> rather than ArrowFormat so this
    // can be passed into ListingOptions.create
    let format: Arc<dyn FileFormat> = Arc::new(ArrowFormat::default());
    Box::into_raw(Box::new(format)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ArrowFormat_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<dyn FileFormat>) };
}
