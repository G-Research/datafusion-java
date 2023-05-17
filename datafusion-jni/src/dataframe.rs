use arrow::ipc::writer::FileWriter;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::convert::Into;
use std::io::BufWriter;
use std::io::Cursor;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::util::{call_error_handler, set_callback_result};

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_collectDataframe(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &*(dataframe as *const DataFrame) };
    let schema = dataframe.schema().into();
    runtime.block_on(async {
        let batches = dataframe.clone().collect().await;
        let batches = match batches {
            Ok(batches) => batches,
            Err(err) => {
                let err_message = env
                    .new_string(err.to_string())
                    .expect("Couldn't create java string for error message");
                let buff = Cursor::new(vec![0; 0]);
                let ba = env
                    .byte_array_from_slice(&buff.get_ref())
                    .expect("cannot create empty byte array");
                env.call_method(
                    callback,
                    "accept",
                    "(Ljava/lang/Object;Ljava/lang/Object;)V",
                    &[(&err_message).into(), (&ba).into()],
                )
                .expect("failed to call method");
                return;
            }
        };
        let mut buff = Cursor::new(vec![0; 0]);
        {
            let mut writer = FileWriter::try_new(BufWriter::new(&mut buff), &schema)
                .expect("failed to create writer");
            for batch in batches {
                writer.write(&batch).expect("failed to write batch");
            }
            writer.finish().expect("failed to finish");
        }
        let err_message = env
            .new_string("".to_string())
            .expect("Couldn't create java string!");
        let ba = env
            .byte_array_from_slice(&buff.get_ref())
            .expect("cannot create byte array");
        env.call_method(
            callback,
            "accept",
            "(Ljava/lang/Object;Ljava/lang/Object;)V",
            &[(&err_message).into(), (&ba).into()],
        )
        .expect("failed to call method");
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_executeStream(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &*(runtime as *const Runtime) };
    let dataframe = unsafe { &*(dataframe as *const DataFrame) };
    runtime.block_on(async {
        let stream_result = dataframe.clone().execute_stream().await;
        set_callback_result(
            &mut env,
            callback,
            stream_result.map(|stream| Box::into_raw(Box::new(stream))),
        );
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_showDataframe(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &*(dataframe as *const DataFrame) };
    runtime.block_on(async {
        let result = dataframe.clone().show().await;
        call_error_handler(&mut env, callback, result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_writeParquet(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &*(dataframe as *const DataFrame) };
    let path: String = env
        .get_string(&path)
        .expect("Couldn't get path as string!")
        .into();
    runtime.block_on(async {
        let result = dataframe.clone().write_parquet(&path, None).await;
        call_error_handler(&mut env, callback, result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_writeCsv(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    path: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &*(dataframe as *const DataFrame) };
    let path: String = env
        .get_string(&path)
        .expect("Couldn't get path as string!")
        .into();
    runtime.block_on(async {
        let result = dataframe.clone().write_csv(&path).await;
        call_error_handler(&mut env, callback, result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_registerTable(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    session: jlong,
    name: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let dataframe = unsafe { &*(dataframe as *const DataFrame) };
    let context = unsafe { &mut *(session as *mut SessionContext) };
    let name: String = env
        .get_string(&name)
        .expect("Couldn't get name as string!")
        .into();
    runtime.block_on(async {
        let result = context
            .register_table(name.as_str(), dataframe.clone().into_view())
            .map(|_| ());
        call_error_handler(&mut env, callback, result);
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DataFrames_destroyDataFrame(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut Arc<DataFrame>) };
}
