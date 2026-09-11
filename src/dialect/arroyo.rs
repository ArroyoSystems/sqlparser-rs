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

use crate::dialect::{Dialect, PostgreSqlDialect, Precedence};
use crate::parser::{Parser, ParserError};

/// Arroyo's streaming SQL dialect: PostgreSQL-style expressions with
/// angle-bracket struct types and generated columns that do not require STORED.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ArroyoDialect {}

impl Dialect for ArroyoDialect {
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_ascii_digit() || ch == '$' || ch == '_'
    }

    fn is_custom_operator_part(&self, ch: char) -> bool {
        PostgreSqlDialect {}.is_custom_operator_part(ch)
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        PostgreSqlDialect {}.get_next_precedence(parser)
    }

    fn prec_value(&self, prec: Precedence) -> u8 {
        PostgreSqlDialect {}.prec_value(prec)
    }

    fn supports_pg_math_prefix_operators(&self) -> bool {
        true
    }

    fn supports_caret_exponentiation(&self) -> bool {
        true
    }

    fn supports_sharp_bitwise_xor(&self) -> bool {
        true
    }

    fn supports_array_overlap_operator(&self) -> bool {
        true
    }

    fn supports_starts_with_operator(&self) -> bool {
        true
    }

    fn supports_escaped_string_literal(&self) -> bool {
        true
    }

    fn supports_unnest_table_factor(&self) -> bool {
        true
    }

    fn supports_unicode_string_literal(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn allow_extract_custom(&self) -> bool {
        true
    }

    fn allow_extract_single_quotes(&self) -> bool {
        true
    }

    fn supports_factorial_operator(&self) -> bool {
        true
    }

    fn supports_bitwise_shift_operators(&self) -> bool {
        true
    }

    fn supports_comment_on(&self) -> bool {
        true
    }

    fn supports_empty_projections(&self) -> bool {
        true
    }

    fn supports_nested_comments(&self) -> bool {
        true
    }

    fn supports_string_escape_constant(&self) -> bool {
        true
    }

    fn supports_numeric_literal_underscores(&self) -> bool {
        true
    }

    fn supports_array_typedef_with_brackets(&self) -> bool {
        true
    }

    fn supports_geometric_types(&self) -> bool {
        true
    }

    fn supports_struct_literal(&self) -> bool {
        true
    }

    fn supports_insert_table_alias(&self) -> bool {
        true
    }
}
