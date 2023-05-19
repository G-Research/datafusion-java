use datafusion::catalog::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig};
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::util::call_error_handler;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_registerCsv(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    pointer: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let name: String = env
        .get_string(&name)
        .expect("Couldn't get name as string!")
        .into();
    let path: String = env
        .get_string(&path)
        .expect("Couldn't get name as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let register_result = context
            .register_csv(&name, &path, CsvReadOptions::new())
            .await;
        call_error_handler(&mut env, callback, register_result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_registerParquet(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    pointer: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let name: String = env
        .get_string(&name)
        .expect("Couldn't get name as string!")
        .into();
    let path: String = env
        .get_string(&path)
        .expect("Couldn't get path as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let register_result = context
            .register_parquet(&name, &path, ParquetReadOptions::default())
            .await;
        call_error_handler(&mut env, callback, register_result);
    });
}

/// Register a table provider as a named table in the session context
#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_registerTable<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    pointer: jlong,
    table_reference: JString<'local>,
    table_provider: jlong,
) -> JString<'local> {
    let table_reference: String = env
        .get_string(&table_reference)
        .expect("Couldn't get table_reference as string!")
        .into();
    let context = unsafe { &*(pointer as *const SessionContext) };
    let table_provider = unsafe { &*(table_provider as *const Arc<dyn TableProvider>) };
    let table_reference = TableReference::from(table_reference.as_str()).to_owned();
    let register_result = context.register_table(table_reference, table_provider.clone());
    let error_message = match register_result {
        Ok(_) => "".to_string(),
        Err(err) => err.to_string(),
    };
    env.new_string(error_message)
        .expect("Couldn't create java string!")
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultSessionContext_querySql(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    pointer: jlong,
    sql: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let sql: String = env
        .get_string(&sql)
        .expect("Couldn't get sql as string!")
        .into();
    let context = unsafe { &mut *(pointer as *mut SessionContext) };
    runtime.block_on(async {
        let query_result = context.sql(&sql).await;
        match query_result {
            Ok(v) => {
                let err_message = JObject::null();
                let dataframe = Box::into_raw(Box::new(v)) as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[(&err_message).into(), dataframe.into()],
                )
            }
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string!");
                let dataframe = -1 as jlong;
                env.call_method(
                    callback,
                    "callback",
                    "(Ljava/lang/String;J)V",
                    &[(&err_message).into(), dataframe.into()],
                )
            }
        }
        .expect("failed to call method");
    });
}
#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionContexts_destroySessionContext(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionContext) };
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionContexts_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let context = SessionContext::new();
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_SessionContexts_createSessionContextWithConfig(
    _env: JNIEnv,
    _class: JClass,
    config: jlong,
) -> jlong {
    let config = unsafe { &*(config as *const SessionConfig) };
    let context = SessionContext::with_config(config.clone());
    Box::into_raw(Box::new(context)) as jlong
}
