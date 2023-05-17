use datafusion::datasource::listing::{ListingOptions, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;

use crate::util::{set_callback_result, set_callback_result_error};

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ListingTableConfig_create(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    context: jlong,
    table_path: JString,
    listing_options: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &*(runtime as *const Runtime) };
    let context = unsafe { &*(context as *const SessionContext) };

    let table_path: String = env
        .get_string(&table_path)
        .expect("Couldn't get Java table_path string")
        .into();
    let table_url = ListingTableUrl::parse(table_path);
    let table_url = match table_url {
        Ok(url) => url,
        Err(err) => {
            set_callback_result_error(&mut env, callback, &err);
            return;
        }
    };
    runtime.block_on(async {
        let listing_table_config = ListingTableConfig::new(table_url);

        let listing_table_config = match listing_options {
            0 => listing_table_config,
            listing_options => {
                let listing_options = unsafe { &*(listing_options as *const ListingOptions) };
                listing_table_config.with_listing_options(listing_options.clone())
            }
        };

        let session_state = context.state();
        let config_result = listing_table_config.infer_schema(&session_state).await;
        set_callback_result(
            &mut env,
            callback,
            config_result.map(|config| Box::into_raw(Box::new(config))),
        );
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
