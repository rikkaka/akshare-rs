use std::collections::HashMap;
use anyhow::Result;

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