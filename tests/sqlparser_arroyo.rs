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

use sqlparser::ast::{BinaryOperator, ColumnOption, Expr, Spanned, Statement, TableConstraint};
use sqlparser::dialect::{dialect_from_str, ArroyoDialect, GenericDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use sqlparser::test_utils::TestedDialects;

fn arroyo() -> TestedDialects {
    TestedDialects::new(vec![Box::new(ArroyoDialect {})])
}

#[test]
fn postgres_expressions_and_struct_types() {
    for sql in [
        "SELECT 2 ^ 3, 7 # 2, 1 << 2, 8 >> 1",
        "SELECT |/9, ||/27, @(-1), !!5",
        "SELECT ARRAY[1] && ARRAY[2], 'hello' ^@ 'he'",
        r#"SELECT E'hello\nworld', U&'hello', "quoted""#,
        "SELECT * FROM UNNEST(ARRAY[1, 2]) WITH ORDINALITY AS t (value, ordinal)",
        "CREATE TABLE events (payload STRUCT<id INT, name TEXT>)",
        "INSERT INTO events AS e SELECT * FROM source",
    ] {
        arroyo().verified_stmt(sql);
    }

    let Expr::BinaryOp { op, .. } = arroyo().verified_expr("2 ^ 3") else {
        panic!("expected binary expression");
    };
    assert_eq!(op, BinaryOperator::PGExp);
    assert!(Parser::parse_sql(&ArroyoDialect {}, "SELECT 1_000").is_ok());
    assert!(dialect_from_str("arroyo").unwrap().is::<ArroyoDialect>());
}

#[test]
fn generated_columns_do_not_require_stored() {
    let sql =
        "CREATE TABLE events (raw TEXT, ts TIMESTAMP GENERATED ALWAYS AS (CAST(raw AS TIMESTAMP)))";
    arroyo().verified_stmt(sql);
    assert!(Parser::parse_sql(&PostgreSqlDialect {}, sql).is_err());
}

#[test]
fn watermark_constraints_round_trip() {
    let dialects = TestedDialects::new(vec![Box::new(ArroyoDialect {}), Box::new(GenericDialect)]);
    for expression in [None, Some("ts - INTERVAL '5 seconds'")] {
        let suffix = expression.map(|e| format!(" AS {e}")).unwrap_or_default();
        let sql = format!("CREATE TABLE events (ts TIMESTAMP, WATERMARK FOR ts{suffix}) WITH (connector = 'kafka')");
        let Statement::CreateTable(table) = dialects.verified_stmt(&sql) else {
            panic!("expected CREATE TABLE");
        };
        let [constraint @ TableConstraint::Watermark {
            column_name,
            watermark_expr,
        }] = table.constraints.as_slice()
        else {
            panic!("expected one watermark constraint");
        };
        assert_eq!(column_name.value, "ts");
        assert_eq!(
            *watermark_expr,
            expression.map(|e| arroyo().verified_expr(e))
        );
        assert_eq!(
            constraint.span(),
            column_name
                .span
                .union_opt(&watermark_expr.as_ref().map(Spanned::span))
        );
        assert!(Parser::parse_sql(&PostgreSqlDialect {}, &sql).is_err());
    }
    arroyo().verified_stmt(
        r#"CREATE TABLE events ("event time" TIMESTAMP, WATERMARK FOR "event time")"#,
    );
}

#[test]
fn invalid_watermark_constraints() {
    for sql in [
        "CREATE TABLE events (ts TIMESTAMP, WATERMARK ts)",
        "CREATE TABLE events (ts TIMESTAMP, WATERMARK FOR)",
        "CREATE TABLE events (ts TIMESTAMP, WATERMARK FOR ts AS)",
        "CREATE TABLE events (ts TIMESTAMP, CONSTRAINT wm WATERMARK FOR ts)",
    ] {
        assert!(Parser::parse_sql(&ArroyoDialect {}, sql).is_err(), "{sql}");
    }
}

#[test]
fn metadata_fields_round_trip() {
    let dialects = TestedDialects::new(vec![Box::new(ArroyoDialect {}), Box::new(GenericDialect)]);
    for (literal, key) in [("'topic'", "topic"), ("'it''s a key'", "it's a key")] {
        let sql = format!(
            "CREATE TABLE logs (id INT, topic TEXT METADATA FROM {literal} NOT NULL, payload TEXT)"
        );
        dialects.verified_stmt(&sql);
        // The round-trip helper intentionally discards token spans.
        let Statement::CreateTable(table) = Parser::parse_sql(&ArroyoDialect {}, &sql)
            .unwrap()
            .remove(0)
        else {
            panic!("expected CREATE TABLE");
        };
        assert_eq!(table.columns.len(), 3);
        let options = &table.columns[1].options;
        let ColumnOption::MetadataField(actual, span) = &options[0].option else {
            panic!("expected metadata field");
        };
        assert_eq!(actual, key);
        assert_eq!(options[0].span(), *span);
        assert_eq!(span.end.column - span.start.column, literal.len() as u64);
        assert_eq!(options[1].option, ColumnOption::NotNull);
        assert!(Parser::parse_sql(&PostgreSqlDialect {}, &sql).is_err());
    }
}

#[test]
fn invalid_metadata_fields() {
    for sql in [
        "CREATE TABLE logs (topic TEXT METADATA)",
        "CREATE TABLE logs (topic TEXT METADATA FROM)",
        "CREATE TABLE logs (topic TEXT METADATA FROM topic)",
        "CREATE TABLE logs (topic TEXT METADATA FROM 42)",
    ] {
        assert!(Parser::parse_sql(&ArroyoDialect {}, sql).is_err(), "{sql}");
    }
}
