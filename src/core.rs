use anyhow::{bail, Ok, Result};
use polars::{datatypes::DataType, prelude::*};
use serde_json::Value;
use std::{collections::HashMap, vec};

use crate::utils::*;

// #[derive(thiserror::Error, Debug)]
// enum Error {
//     #[error("No data is found on the args")]
//     NoData,
// }

pub async fn stock_zh_a_hist(
    symbol: &str,
    period: &str,
    start_date: &str,
    end_date: &str,
    adjust: &str,
) -> Result<Option<DataFrame>> {
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
    let params: HashMap<&str, &str> = vec![
        ("fields1", "f1,f2,f3,f4,f5,f6"),
        (
            "fields2",
            "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        ),
        ("ut", "7eea3edcaed734bea9cbfc24409ed989"),
        ("klt", period),
        ("fqt", adjust),
        ("secid", format!("{symbol}").as_str()),
        ("beg", start_date),
        ("end", end_date),
        ("_", "1623766962675"),
    ]
    .into_iter()
    .collect();

    let r: Value = request(url, params).await?.parse()?;
    if r["data"]["klines"] == Value::Null {
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

    let klines = r["data"]["klines"]
        .as_array()
        .unwrap()
        .into_iter()
        .map(|x| x.as_str().unwrap().split(',').collect::<Vec<&str>>())
        .collect::<Vec<Vec<&str>>>();

    let series = columns
        .iter()
        .zip(klines.transpose().iter())
        .map(|(x, y)| Series::new(x, y))
        .collect::<Vec<Series>>();

    let tmp_df = DataFrame::new(series)?;

    let mut col_iter = columns.iter();
    let mut new_df = DataFrame::new(Vec::<Series>::new())?;
    new_df.with_column(
        tmp_df
            .column(col_iter.next().unwrap())?
            .utf8()?
            .as_date(Some("%Y-%m-%d"), true)?,
    )?;
    for col in col_iter {
        new_df.with_column(tmp_df.column(col)?.cast(&DataType::Float64)?)?;
    }

    Ok(Some(new_df))
}
