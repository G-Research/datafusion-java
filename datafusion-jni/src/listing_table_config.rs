use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingTableConfig_create(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    context: jlong,
    table_path: JString,
    listing_options: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let context = unsafe { &mut *(context as *mut SessionContext) };

    let table_path: String = env
        .get_string(table_path)
        .expect("Couldn't get Java table_path string")
        .into();
    let table_url = ListingTableUrl::parse(table_path);
    let table_url = match table_url {
        Ok(url) => url,
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string");
            let config_id = -1 as jlong;
            env.call_method(
                callback,
                "callback",
                "(Ljava/lang/String;J)V",
                &[err_message.into(), config_id.into()],
            )
            .expect("failed to call method");
            return;
        }
    };
    runtime.block_on(async {
        let listing_table_config = ListingTableConfig::new(table_url);

        let listing_table_config = match listing_options {
            0 => listing_table_config,
            listing_options => {
                let listing_options = unsafe { &mut *(listing_options as *mut ListingOptions) };
                listing_table_config.with_listing_options(listing_options.clone())
            }
        };

        let session_state = context.state();
        match listing_table_config.infer_schema(&session_state).await {
            Ok(config) => {
                let config_id = Box::into_raw(Box::new(config)) as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[JValue::Void, config_id.into()],
                )
            }
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string");
                let config_id = -1 as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[err_message.into(), config_id.into()],
                )
            }
        }
        .expect("failed to callback method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingTableConfig_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut ListingTableConfig) };
}
