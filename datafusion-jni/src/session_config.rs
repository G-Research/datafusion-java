use datafusion::execution::context::SessionConfig;
use jni::objects::{JClass, JObject};
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
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionConfig) };
}

// Helper macros to implement boolean options

macro_rules! bool_getter {
    ($name:ident, $($property_path:ident).+) => {
        #[no_mangle]
        pub extern "system" fn $name(
            _env: JNIEnv,
            _class: JClass,
            pointer: jlong,
        ) -> jboolean {
            let config = unsafe { &*(pointer as *const SessionConfig) };
            let property_value = config.options().$($property_path).+;
            if property_value {
                1u8
            } else {
                0u8
            }
        }
    }
}

macro_rules! bool_setter {
    ($name:ident, $($property_path:ident).+) => {
        #[no_mangle]
        pub extern "system" fn $name(
            _env: JNIEnv,
            _class: JClass,
            pointer: jlong,
            enabled: jboolean,
        ) {
            let config = unsafe { &mut *(pointer as *mut SessionConfig) };
            config.options_mut().$($property_path).+ = enabled != 0u8;
        }
    }
}

// ParquetOptions

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsEnablePageIndex,
    execution.parquet.enable_page_index
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsEnablePageIndex,
    execution.parquet.enable_page_index
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsPruning,
    execution.parquet.pruning
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsPruning,
    execution.parquet.pruning
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsSkipMetadata,
    execution.parquet.skip_metadata
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsSkipMetadata,
    execution.parquet.skip_metadata
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsPushdownFilters,
    execution.parquet.pushdown_filters
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsPushdownFilters,
    execution.parquet.pushdown_filters
);

bool_getter!(
    Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsReorderFilters,
    execution.parquet.reorder_filters
);
bool_setter!(
    Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsReorderFilters,
    execution.parquet.reorder_filters
);

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_getParquetOptionsMetadataSizeHint(
    mut env: JNIEnv,
    _class: JClass,
    pointer: jlong,
    on_value: JObject,
) {
    let config = unsafe { &*(pointer as *const SessionConfig) };
    let size_hint = config.options().execution.parquet.metadata_size_hint;
    match size_hint {
        Some(size_hint) => {
            env.call_method(on_value, "accept", "(J)V", &[(size_hint as jlong).into()])
                .expect("failed to call method");
        }
        None => {}
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionConfig_setParquetOptionsMetadataSizeHint(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
    has_value: jboolean,
    value: jlong,
) {
    let config = unsafe { &mut *(pointer as *mut SessionConfig) };
    if has_value == 1u8 {
        config.options_mut().execution.parquet.metadata_size_hint = Some(value as usize);
    } else {
        config.options_mut().execution.parquet.metadata_size_hint = None;
    }
}