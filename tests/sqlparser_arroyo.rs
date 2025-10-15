// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]

use sqlparser::ast::{BinaryOperator, ColumnOption, Expr, Ident, Statement, TableConstraint};
use sqlparser::dialect::ArroyoDialect;
use sqlparser::parser::Parser;
use sqlparser::test_utils;
use sqlparser::tokenizer::{Location, Span};

#[test]
fn test_watermark_with_expr() {
    let sql = "CREATE TABLE orders (
        customer_id INT,
        order_id INT,
        date_string TEXT,
        timestamp TIMESTAMP GENERATED ALWAYS AS (CAST(date_string as TIMESTAMP)),
        WATERMARK FOR timestamp AS timestamp + 5
    ) WITH (
        connector = 'kafka',
        format = 'json',
        type = 'source',
        bootstrap_servers = 'localhost:9092',
        topic = 'order_topic'
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(
        ct.constraints,
        vec![TableConstraint::Watermark {
            column_name: Ident::new("timestamp"),
            watermark_expr: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("timestamp"))),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Value(
                    test_utils::number("5")
                        .with_span(Span::new(Location::new(5, 4), Location::new(5, 10)))
                )),
            }),
        }]
    );
}

#[test]
fn test_watermark_without_expr() {
    let sql = "CREATE TABLE users (
        customer_id INT,
        timestamp TIMESTAMP,
        WATERMARK FOR timestamp
    ) WITH (
        connector = 'kafka',
        format = 'json',
        type = 'source',
        bootstrap_servers = 'localhost:9092',
        topic = 'order_topic'
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(
        ct.constraints,
        vec![TableConstraint::Watermark {
            column_name: Ident::new("timestamp"),
            watermark_expr: None,
        }]
    );
}

#[test]
fn test_metadata_field() {
    let sql = "CREATE TABLE logs (
        id TEXT,
        kafka_topic STRING METADATA FROM 'topic',
        log TEXT
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    assert_eq!(ct.columns.len(), 3);

    // Check the middle column with METADATA FROM
    let column = &ct.columns[1];
    assert_eq!(column.name, Ident::new("kafka_topic"));

    // Check for the METADATA FROM option
    let mut found_metadata = false;
    for option_def in &column.options {
        if let ColumnOption::MetadataField(key, _) = &option_def.option {
            found_metadata = true;
            assert_eq!(key, "topic");
        }
    }

    assert!(
        found_metadata,
        "Expected METADATA FROM option in column definition"
    );
}

#[test]
fn test_iceberg_partitioned_by() {
    let sql = "CREATE TABLE ice (
        ts TIMESTAMP NOT NULL,
        id INT NOT NULL,
        favorite_color TEXT
    ) WITH (
        connector = 'iceberg',
        format = 'parquet',
        table_name = 'arroyo_test'
    ) PARTITIONED BY (
        hour(ts),
        bucket(32, id),
        truncate(8, favorite_color)
    )";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    // Verify basic structure
    assert_eq!(ct.name.to_string(), "ice");
    assert_eq!(ct.columns.len(), 3);

    // Verify arroyo_partitions is present
    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions to be Some");
    assert_eq!(partitions.len(), 3);

    // Check each partition transform
    // hour(ts)
    match &partitions[0] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "hour");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 1);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for hour(ts)"),
    }

    // bucket(32, id)
    match &partitions[1] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "bucket");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 2);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for bucket(32, id)"),
    }

    // truncate(8, favorite_color)
    match &partitions[2] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "truncate");
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                assert_eq!(list.args.len(), 2);
            } else {
                panic!("Expected List arguments");
            }
        }
        _ => panic!("Expected Function for truncate(8, favorite_color)"),
    }

    // Test round-trip: the formatted output should parse back to the same structure
    let formatted = ct.to_string();
    let reparsed = Parser::parse_sql(&ArroyoDialect {}, &formatted).unwrap();
    let Statement::CreateTable(ct2) = reparsed.get(0).unwrap() else {
        panic!("not create table on reparse")
    };

    assert_eq!(ct.arroyo_partitions, ct2.arroyo_partitions);
}

#[test]
fn test_iceberg_partitioned_by_single() {
    let sql = "CREATE TABLE events (
        event_time TIMESTAMP
    ) WITH (
        connector = 'iceberg'
    ) PARTITIONED BY (day(event_time))";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions");
    assert_eq!(partitions.len(), 1);

    match &partitions[0] {
        Expr::Function(f) => {
            assert_eq!(f.name.to_string(), "day");
        }
        _ => panic!("Expected Function for day(event_time)"),
    }
}

#[test]
fn test_iceberg_partitioned_by_identity() {
    // Test identity transform (just a column name)
    let sql = "CREATE TABLE data (
        region TEXT,
        value INT
    ) WITH (
        connector = 'iceberg'
    ) PARTITIONED BY (region)";

    let parse = Parser::parse_sql(&ArroyoDialect {}, sql).unwrap();
    let Statement::CreateTable(ct) = parse.get(0).unwrap() else {
        panic!("not create table")
    };

    let partitions = ct
        .arroyo_partitions
        .as_ref()
        .expect("Expected arroyo_partitions");
    assert_eq!(partitions.len(), 1);

    match &partitions[0] {
        Expr::Identifier(ident) => {
            assert_eq!(ident.value, "region");
        }
        _ => panic!("Expected Identifier for region"),
    }
}
