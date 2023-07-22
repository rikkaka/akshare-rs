use anyhow::{bail, Result};
use polars::{frame::row::Row, prelude::*};
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::HashMap;

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

pub trait Transpose {
    fn transpose(self) -> Self;
}

impl<T: Clone> Transpose for Vec<Vec<T>> {
    fn transpose(self) -> Self {
        let mut new_matrix = Vec::new();
        for i in 0..self[0].len() {
            let mut column = Vec::new();
            for row in self.iter() {
                column.push(row[i].clone());
            }
            new_matrix.push(column);
        }
        new_matrix
    }
}

type Iter<'a, T> = Box<dyn Iterator<Item = T> + 'a>;
type IIter<'a, T> = Box<dyn Iterator<Item = Iter<'a, T>>>;

pub fn lines_str_split<'a>(value: &'a Value, sep: char) -> IIterAnyValue<'a> {
    Box::new(value.as_array().unwrap().iter().map(move |x| {
        Box::new(
            x.as_str()
                .unwrap()
                .split(sep)
                .map(move |x| AnyValue::Utf8(x))
                .into_iter(),
        ) as Iter<AnyValue>
    }))
}

pub fn lines_array<'a>(value: &'a Value) -> IIterAnyValue<'a> {
    Box::new(value.as_array().unwrap().iter().map(|x| {
        Box::new(x.as_array().unwrap().iter().map(|x| value_to_anyvalue(x))) as Iter<AnyValue>
    }))
}

type IterAnyValue<'a> = Box<dyn Iterator<Item = AnyValue<'a>> + 'a>;
type IIterAnyValue<'a> = Box<dyn Iterator<Item = IterAnyValue<'a>> + 'a>;
pub fn iter2d_to_df<'a>(iter: IIterAnyValue<'a>, schema: &Schema) -> Result<DataFrame> {
    let rows = iter.map(|x| Row::new(x.collect())).collect::<Vec<Row>>();
    Ok(DataFrame::from_rows_and_schema(&rows, schema)?)
}

pub fn array_object_to_df(value: &Value, schema: &Schema) -> Result<DataFrame> {
    iter2d_to_df(
        Box::new(value.as_array().unwrap().iter().map(|x| {
            Box::new(
                x.as_object()
                    .unwrap()
                    .iter()
                    .map(|(_, x)| value_to_anyvalue(x)),
            ) as IterAnyValue
        })),
        schema,
    )
}

pub fn array_object_to_seriess(names: &[&str], value: &Value) -> Vec<Series> {
    value
        .as_array()
        .unwrap()
        .iter()
        .zip(names)
        .map(|(x, name)| {
            Series::new(
                name,
                x.as_object()
                    .unwrap()
                    .iter()
                    .map(|(_, x)| value_to_anyvalue(x))
                    .collect::<Vec<AnyValue>>(),
            )
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
        Value::String(x) => AnyValue::Utf8(x),
        _ => panic!("not support type"),
    }
}
