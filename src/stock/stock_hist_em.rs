use crate::imports::*;

pub async fn stock_zh_a_spot_em() -> Result<Option<DataFrame>> {
    let url = "http://82.push2.eastmoney.com/api/qt/clist/get";
    let params = hashmap! {
        "pn" => "1",
        "pz" => "5000",
        "po" => "1",
        "np" => "1",
        "ut" => "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt" => "2",
        "invt" => "2",
        "fid" => "f3",
        "fs" => "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
        "fields" => "f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11",
        "_" => "1623833739532",
    };
    let data_json = request(url, params).await?;
    if data_json["data"]["diff"] == Value::Null {
        return Ok(None);
    };

    let columns = vec![
        "_",
        "最新价",
        "涨跌幅",
        "涨跌额",
        "成交量",
        "成交额",
        "振幅",
        "换手率",
        "市盈率-动态",
        "量比",
        "5分钟涨跌",
        "代码",
        "_",
        "名称",
        "最高",
        "最低",
        "今开",
        "昨收",
        "总市值",
        "流通市值",
        "涨速",
        "市净率",
        "60日涨跌幅",
        "年初至今涨跌幅",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ];

    let temp_df = array_object_to_df(&columns, &data_json["data"]["diff"])?;

    println!("{:?}", temp_df);

    todo!()
}

pub async fn stock_zh_a_hist(
    symbol: &str,
    period: &str,
    start_date: &str,
    end_date: &str,
    adjust: &str,
) -> Result<Option<DataFrame>> {
    let code_id_map = code_id_map_em().await;

    let period = match period {
        "daily" => "101",
        "weekly" => "102",
        "monthly" => "103",
        _ => bail!("period must be daily, weekly or monthly"),
    };

    let adjust = match adjust {
        "qfq" => "1",
        "hfq" => "2",
        "" => "0",
        _ => bail!("adjust must be qfq, hfq or empty"),
    };

    let url = "http://push2his.eastmoney.com/api/qt/stock/kline/get";
    let secid = format!("{}.{}", code_id_map[symbol], symbol);
    let params = hashmap! {
        "fields1" => "f1,f2,f3,f4,f5,f6",
        "fields2" => "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut" => "7eea3edcaed734bea9cbfc24409ed989",
        "klt" => period,
        "fqt" => adjust,
        "secid" => secid.as_str(),
        "beg" => start_date,
        "end" => end_date,
        "_" => "1623766962675",
    };

    let data_json: Value = request(url, params).await?;
    if data_json["data"]["klines"] == Value::Null {
        return Ok(None);
    };

    let columns = vec![
        "日期",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "振幅",
        "涨跌幅",
        "涨跌额",
        "换手率",
    ];

    let klines = lines_str_split(&data_json["data"]["klines"], ',');
    let temp_df = iter2d_to_df(klines, &Schema::new())?;

    let mut col_iter = columns.iter();
    let mut new_df = DataFrame::new(Vec::<Series>::new())?;
    new_df.with_column(
        temp_df
            .column(col_iter.next().unwrap())?
            .utf8()?
            .as_date(Some("%Y-%m-%d"), true)?,
    )?;
    for col in col_iter {
        new_df.with_column(temp_df.column(col)?.cast(&DataType::Float64)?)?;
    }

    Ok(Some(new_df))
}

// 60 * 60 * 24
#[cached(time = 86400)]
async fn code_id_map_em() -> AHashMap<String, char> {
    let mut result = AHashMap::with_capacity(6000);
    for (fs, id) in vec![
        ("m:1 t:2,m:1 t:23", '1'),
        ("m:0 t:6,m:0 t:80", '0'),
        ("m:0 t:81 s:2048", '0'),
    ] {
        result.extend(code_in_map(fs, id).await)
    }

    result
}

async fn code_in_map(fs: &str, id: char) -> Vec<(String, char)> {
    let url = "http://80.push2.eastmoney.com/api/qt/clist/get";
    let params = hashmap! {
        "pn" => "1",
        "pz" => "50000",
        "po" => "1",
        "np" => "1",
        "ut" => "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt" => "2",
        "invt" => "2",
        "fid" => "f3",
        "fs" => fs,
        "fields" => "f12",
        "_" => "1623833739532",
    };
    let data_json: Value = request(url, params).await.unwrap();
    // if data_json["data"]["diff"] == Value::Null {
    //     return ;
    // };
    data_json["data"]["diff"]
        .as_array()
        .unwrap()
        .into_iter()
        .map(|x| (x["f12"].as_str().unwrap().to_owned(), id))
        .collect::<Vec<(String, char)>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_code_id_map_em() {
        let now = Instant::now();
        let res = code_id_map_em().await;
        assert!(!res.is_empty());
        println!("code_id_map_em: {:?}", now.elapsed());
    }

    #[tokio::test]
    async fn test_stock_zh_a_spot_em() {
        let now = Instant::now();
        let res = stock_zh_a_spot_em().await.unwrap().unwrap();
        assert!(!res.is_empty());
        println!("stock_zh_a_spot_em: {:?}", now.elapsed());
        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_stock_zh_a_hist() {
        let now = Instant::now();
        let res = stock_zh_a_hist("000001", "daily", "20210601", "20210615", "qfq")
            .await
            .unwrap()
            .unwrap();
        assert!(!res.is_empty());
        println!("time: {:?}", now.elapsed());
        println!("{:?}", res);
    }
}
