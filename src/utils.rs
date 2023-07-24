use polars::frame::row::Row;

use crate::imports::*;

pub async fn request(url: &str, params: HashMap<&str, &str>) -> Result<Value> {
    let client = reqwest::Client::new();
    let resp = client.get(url).query(&params).send().await?;
    let text = resp.text().await?;
    Ok(text.parse()?)
}

pub async fn request_header(
    url: &str,
    params: HashMap<&str, &str>,
    headers: HeaderMap,
) -> Result<Value> {
    let client = reqwest::Client::new();
    let resp = client
        .get(url)
        .query(&params)
        .headers(headers)
        .send()
        .await?;
    let text = resp.text().await?;
    Ok(text.parse()?)
}

// type Iter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;
// type IIter<'a, T> = Box<dyn Iterator<Item = Iter<'a, T>>>;

pub fn array_split_to_df<'a>(value: &'a Value, sep: char, schema: &Schema) -> DataFrame {
    // Box::new(value.as_array().unwrap().iter().map(move |x| {
    //     Box::new(
    //         x.as_str()
    //             .unwrap()
    //             .split(sep)
    //             .map(move |x| AnyValue::Utf8(x))
    //     ) as IterAnyValue
    // }))
    let rows = value
        .as_array()
        .unwrap()
        .par_iter()
        .map(|x| {
            Row::new(
                x.as_str()
                    .expect("Mismatched value type")
                    .split(sep)
                    .map(|x| AnyValue::Utf8(x))
                    .collect::<Vec<AnyValue>>(),
            )
        })
        .collect::<Vec<Row>>();
    DataFrame::from_rows_and_schema(&rows, schema).unwrap()
}

// pub fn array_array_to_iiter<'a>(value: &'a Value) -> IIterAnyValue<'a> {
//     Box::new(value.as_array().unwrap().iter().map(|x| {
//         Box::new(x.as_array().unwrap().iter().map(|x| value_to_anyvalue(x))) as Iter<AnyValue>
//     }))
// }

// type IterAnyValue<'a> = Box<dyn Iterator<Item = AnyValue<'a>> + 'a>;
// type IIterAnyValue<'a> = Box<dyn Iterator<Item = IterAnyValue<'a>> + 'a>;
// pub fn iiter_to_df<'a>(iter: IIterAnyValue<'a>, schema: &Schema) -> Result<DataFrame> {
//     let rows = iter.map(|x| Row::new(x.collect())).collect::<Vec<Row>>();
//     Ok(DataFrame::from_rows_and_schema(&rows, schema)?)
// }

// 将每个array作为一行转换为DataFrame
pub fn array_object_to_df_rows(value: &Value, schema: &Schema) -> DataFrame {
    let rows = value
        .as_array()
        .expect("Mismatched value type")
        .par_iter()
        .map(|x| {
            Row::new(
                x.as_object()
                    .unwrap()
                    .iter()
                    .map(|(_, x)| value_to_anyvalue(x))
                    .collect::<Vec<AnyValue>>(),
            )
        })
        .collect::<Vec<Row>>();
    DataFrame::from_rows_and_schema(&rows, schema).unwrap()
}

// 将每个array作为一列转换为DataFrame
pub fn array_object_to_df_cols(value: &Value, schema: &Schema) -> DataFrame {
    let seriess = array_object_to_seriess(value, schema);
    DataFrame::new(seriess).unwrap()
}

pub fn array_object_to_seriess(value: &Value, schema: &Schema) -> Vec<Series> {
    value
        .as_array()
        .expect("Mismatched value type")
        .iter()
        .zip(schema.iter_fields())
        .map(|(x, field)| {
            Series::from_any_values_and_dtype(
                &field.name,
                &x.as_object()
                    .unwrap()
                    .iter()
                    .map(|(_, x)| value_to_anyvalue(x))
                    .collect::<Vec<AnyValue>>(),
                &field.dtype,
                true,
            ).unwrap()
        })
        .collect::<Vec<Series>>()
}

fn value_to_anyvalue(value: &Value) -> AnyValue {
    match value {
        Value::Null => AnyValue::Null,
        Value::Bool(x) => AnyValue::Boolean(*x),
        Value::Number(x) => {
            if x.is_i64() {
                AnyValue::Int64(x.as_i64().unwrap())
            } else {
                AnyValue::Float64(x.as_f64().unwrap())
            }
        }
        Value::String(x) => match x.as_str() {
            "-" | "" => AnyValue::Null,
            _ => AnyValue::Utf8(x),
        },
        _ => panic!("not support type"),
    }
}

pub fn columns_to_schema(columns: &[&str], dtype: DataType) -> Schema {
    let mut schema = Schema::with_capacity(columns.len());
    for i in 0..columns.len() {
        schema
            .insert_at_index(i, columns[i].into(), dtype.clone())
            .unwrap();
    }
    schema
}