mod utils;



use wasm_bindgen::prelude::*;

use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, sync::Arc};



/// Reads a parquet buffer. You can pass columns to specify which columns should be read
/// It returns data into arrow IPC format.
#[wasm_bindgen]
pub fn from_parquet_buffer(buf: Vec<u8>, columns: Vec<JsValue>) -> Vec<u8> {
    // Create Parquet reader
    let cursor: Bytes = buf.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor).unwrap();

    let mut arrow_schema = builder.schema().clone();

    if columns.len() > 0 {
        let column_positions: Vec<usize> = columns.iter().map(|column_name: &JsValue| arrow_schema.index_of(column_name.as_string().unwrap().as_str()).unwrap()).collect();
        arrow_schema = Arc::new(arrow_schema.project(&column_positions).unwrap());
    }

    // Create Arrow reader
    let reader = builder.build().unwrap();

    // Create IPC Writer
    let mut output_file = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema).unwrap();
        // Iterate over record batches, writing them to IPC stream
        for maybe_record_batch in reader {
            let record_batch = maybe_record_batch.unwrap();
            writer.write(&record_batch).unwrap();
        }
        writer.finish().unwrap();
    }

    output_file
}


#[wasm_bindgen]
pub fn from_parquet_file(path: String, columns: Vec<JsValue>) -> Vec<u8> {
    let file = File::open(path).unwrap();
    // let file_reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    // let schema = file_reader.metadata().file_metadata().schema();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();


    let mut arrow_schema = builder.schema().clone();

    if columns.len() > 0 {
        let column_positions: Vec<usize> = columns.iter().map(|column_name: &JsValue| arrow_schema.index_of(column_name.as_string().unwrap().as_str()).unwrap()).collect();
        arrow_schema = Arc::new(arrow_schema.project(&column_positions).unwrap());
    }

    let  reader = builder.build().unwrap();
    let mut output_file = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut output_file, &arrow_schema).unwrap();

        // Iterate over record batches, writing them to IPC stream
        for maybe_record_batch in reader {
            let record_batch = maybe_record_batch.unwrap();
            writer.write(&record_batch).unwrap();
        }
        writer.finish().unwrap();
    }

    // Note that this returns output_file directly instead of using writer.into_inner().to_vec() as
    // the latter seems likely to incur an extra copy of the vec
    output_file

}