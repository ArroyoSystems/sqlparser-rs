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

use sqlparser::ast::{BinaryOperator, Expr};
use sqlparser::dialect::{dialect_from_str, ArroyoDialect, PostgreSqlDialect};
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
