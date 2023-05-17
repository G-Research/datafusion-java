use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::TableProvider;
use jni::objects::{JClass, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingTable_create(
    env: JNIEnv,
    _class: JClass,
    table_config: jlong,
    object_result: JObject,
) {
    let table_config = unsafe { &mut *(table_config as *mut ListingTableConfig) };
    let table_config = ListingTableConfig {
        table_paths: table_config.table_paths.clone(),
        file_schema: table_config.file_schema.clone(),
        options: table_config.options.clone(),
    };
    match ListingTable::try_new(table_config) {
        Ok(lt) => {
            let table_provider: Arc<dyn TableProvider> = Arc::new(lt);
            let object_id = Box::into_raw(Box::new(table_provider)) as jlong;
            env.call_method(object_result, "setOk", "(J)V", &[object_id.into()])
        }
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string");
            env.call_method(
                object_result,
                "setError",
                "(Ljava/lang/String;)V",
                &[err_message.into()],
            )
        }
    }
    .expect("failed to call method");
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingTable_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<ListingTable>) };
}
