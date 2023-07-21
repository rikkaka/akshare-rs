use std::collections::HashMap;
use anyhow::Result;
use polars::prelude::DataFrame;
use serde_json::Value;

pub async fn request(url: &str, params: HashMap<&str, &str>) -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client.get(url).query(&params).send().await?;
    let text = resp.text().await?;
    Ok(text)
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

pub fn value_line_to_df(value: Value) -> Result<DataFrame> {
    let lines = value.as_array().unwrap().into_iter().map(|x| x.as_array().unwrap().to_vec()).collect::<Vec<_>>();

    todo!()
}