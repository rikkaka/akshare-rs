use anyhow::{bail, Result};
use polars::prelude::*;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::collections::HashMap;

pub async fn request(
    url: &str,
    params: HashMap<&str, &str>,
) -> Result<Value> {
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
    let resp = client.get(url).query(&params).headers(headers).send().await?;
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

pub trait ValueTraits {
    fn lines(&self) -> Vec<&str>;
    fn lines_split(&self, sep: &str) -> Vec<Vec<&str>> {
        self.lines()
            .iter()
            .map(|x| x.split(sep).collect::<Vec<&str>>())
            .collect::<Vec<Vec<&str>>>()
    }
}

impl ValueTraits for Value {
    fn lines(&self) -> Vec<&str> {
        self.as_array()
            .unwrap()
            .into_iter()
            .map(|x| x.as_str().unwrap())
            .collect::<Vec<&str>>()
    }
}

pub fn vecs_to_seriess(names: &Vec<&str>, vecs: Vec<Vec<&str>>) -> Vec<Series> {
    let mut series = Vec::new();
    for (name, vec) in names.iter().zip(vecs.iter()) {
        series.push(Series::new(name, vec));
    }
    series
}

pub fn vecs_to_dataframe(names: &Vec<&str>, vecs: Vec<Vec<&str>>) -> Result<DataFrame> {
    let vecs = vecs.transpose();
    let series = vecs_to_seriess(names, vecs);
    Ok(DataFrame::new(series)?)
}
