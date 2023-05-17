use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingOptions_create(
    env: JNIEnv,
    _class: JClass,
    format: jlong,
    file_extension: JString,
) -> jlong {
    let format = unsafe { &*(format as *const Arc<dyn FileFormat>) };

    let mut listing_options = ListingOptions::new(format.clone());
    listing_options.file_extension = env
        .get_string(file_extension)
        .expect("Couldn't get Java file_extension string")
        .into();
    Box::into_raw(Box::new(listing_options)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingOptions_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut ListingOptions) };
}
