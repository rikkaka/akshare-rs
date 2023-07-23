use crate::imports::*;

pub async fn stock_sse_summary() -> Result<DataFrame> {
    let url = "http://query.sse.com.cn/commonQuery.do";
    let params = hashmap! {
        "sqlId" => "COMMON_SSE_SJ_GPSJ_GPSJZM_TJSJ_L",
        "PRODUCT_NAME" => "股票,主板,科创板",
        "type" => "inParams",
        "_" => "1640855495128",
    };
    let mut headers = HeaderMap::new();
    headers.insert("Referer", "http://www.sse.com.cn/".parse().unwrap());
    headers.insert(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
        AppleWebKit/537.36 (KHTML, like Gecko) \
        Chrome/89.0.4389.90 Safari/537.36"
            .parse()
            .unwrap(),
    );
    let data_json: Value = request_header(url, params, headers).await?;

    let index_vec = [
        "流通股本",
        "总市值",
        "平均市盈率",
        "上市公司",
        "上市股票",
        "流通市值",
        "报告时间",
        "-",
        "总股本",
        "项目",
    ];
    let mut seriess = vec![Series::new("项目", index_vec)];

    let columns = vec!["股票", "主板", "科创板"];
    seriess.extend(array_object_to_seriess(&columns, &data_json["result"]));

    let temp_df = DataFrame::new(seriess)?;
    let temp_df = temp_df
        .lazy()
        .filter(col("项目").neq(lit("-")))
        .filter(col("项目").neq(lit("项目")))
        .collect()?;

    Ok(temp_df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stock_sse_summary() {
        let now = Instant::now();
        let res = stock_sse_summary().await.unwrap();
        assert!(!res.is_empty());
        println!("stock_sse_summary: {:?}", now.elapsed());
        println!("{:?}", res);
    }
}
