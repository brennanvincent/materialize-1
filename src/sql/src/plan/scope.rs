// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Handles SQL's scoping rules.
//!
//! A scope spans a single SQL `Query`. Nested subqueries create new scopes.
//! Names are resolved against the innermost scope first.
//! * If a match is found, it is returned.
//! * If no matches are found, the name is resolved against the parent scope.
//! * If multiple matches are found, the name is ambigious and we return an
//!   error to the user.
//!
//! Matching rules:
//! * `bar` will match any column in the scope named `bar`
//! * `foo.bar` will match any column in the scope named `bar` that originated
//!    from a table named `foo`.
//! * Table aliases such as `foo AS quux` replace the old table name.
//! * Functions create unnamed columns, which can be named with columns aliases
//!   `(bar + 1) as more_bar`.
//!
//! Additionally, most databases fold some form of CSE into name resolution so
//! that eg `SELECT sum(x) FROM foo GROUP BY sum(x)` would be treated something
//! like `SELECT "sum(x)" FROM foo GROUP BY sum(x) AS "sum(x)"` rather than
//! failing to resolve `x`. We handle this by including the underlying
//! `sql_parser::ast::Expr` in cases where this is possible.
//!
//! Many SQL expressions do strange and arbitrary things to scopes. Rather than
//! try to capture them all here, we just expose the internals of `Scope` and
//! handle it in the appropriate place in `super::query`.
//!
//! NOTE(benesch): The above approach of exposing scope's internals to the
//! entire planner has not aged well. SQL scopes are now full of undocumented
//! assumptions and requirements, since various subcomponents of the planner
//! shove data into scope items to communicate with subcomponents a mile away.
//! I've tried to refactor this code several times to no avail. It works better
//! than you might expect. But you have been warned. Tread carefully!

use std::collections::HashSet;

use anyhow::bail;

use repr::ColumnName;

use crate::ast::Expr;
use crate::names::PartialName;
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::plan_utils::JoinSide;
use crate::plan::query::Aug;

#[derive(Debug, Clone)]
pub struct ScopeItem {
    /// The name of the table that produced this scope item, if any.
    pub table_name: Option<PartialName>,
    /// The name of the column, if any.
    ///
    /// If omitted, the scope item will not be resolvable.
    pub column_name: Option<ColumnName>,
    /// The expression from which this scope item is derived. Used by `GROUP
    /// BY`.
    pub expr: Option<Expr<Aug>>,
    /// Whether the column is the return value of a function that produces only
    /// a single column. This accounts for a strange PostgreSQL special case
    /// around whole-row expansion.
    pub from_single_column_function: bool,
    /// Controls whether the column is only accessible via a table-qualified
    /// reference. When false, the scope item is also excluded from `SELECT *`.
    ///
    /// This should be true for almost all scope items. It is set to false for
    /// join columns in USING constraints. For exmaple, in `t1 FULL JOIN t2
    /// USING a`, `t1.a` and `t2.a` are still available by fully-qualified
    /// reference, but a bare `a` refers to a new column whose value is
    /// `coalesce(t1.a, t2.a)`. This is a big special case because normally
    /// having three columns in scope named `a` would result in "ambiguous
    /// column reference" errors.
    pub allow_unqualified_references: bool,
    /// Whether reference the item should produce an error about the item being
    /// on the wrong side of a lateral join.
    ///
    /// Per PostgreSQL (and apparently SQL:2008), we can't simply make these
    /// items unnameable. These items need to *exist* because they might shadow
    /// variables in outer scopes that would otherwise be valid to reference,
    /// but accessing them needs to produce an error.
    pub lateral_error_if_referenced: bool,
    // Force use of the constructor methods.
    _private: (),
}

#[derive(Debug, Clone)]
pub struct Scope {
    // items in this query
    pub items: Vec<ScopeItem>,
    // items inherited from an enclosing query
    pub outer_scope: Option<Box<Scope>>,
    // Whether this scope starts a new chain of lateral outer scopes.
    //
    // It's easiest to understand with an example. Consider this query:
    //
    //     SELECT (SELECT * FROM tab1, tab2, (SELECT tab1.a, tab3.a)) FROM tab3, tab4
    //     Scope 1:                          ------------------------
    //     Scope 2:                    ----
    //     Scope 3:              ----
    //     Scope 4:                                                              ----
    //     Scope 5:                                                        ----
    //
    // Note that the because the derived table is not marked `LATERAL`, its
    // reference to `tab3.a` is valid but its reference to `tab1.a` is not.
    //
    // Scope 5 is the parent of scope 4, scope 4 is the parent of scope 3, and
    // so on. The non-lateral derived table is not allowed to access scopes 2
    // and 3, because they are part of the same lateral chain, but it *is*
    // allowed to access scope 4 and 5. So, to capture this information, we set
    // `lateral_barrier: true` for scope 4.
    pub lateral_barrier: bool,
}

impl ScopeItem {
    fn empty() -> ScopeItem {
        ScopeItem {
            table_name: None,
            column_name: None,
            expr: None,
            from_single_column_function: false,
            allow_unqualified_references: true,
            lateral_error_if_referenced: false,
            _private: (),
        }
    }

    /// Constructs a new scope item from a bare column name.
    pub fn from_column_name(column_name: ColumnName) -> ScopeItem {
        ScopeItem::from_name(None, Some(column_name))
    }

    /// Constructs a new scope item from a name.
    pub fn from_name(
        table_name: Option<PartialName>,
        column_name: Option<ColumnName>,
    ) -> ScopeItem {
        let mut item = ScopeItem::empty();
        item.table_name = table_name;
        item.column_name = column_name;
        item
    }

    /// Constructs a new scope item with no name from an expression.
    pub fn from_expr(expr: impl Into<Option<Expr<Aug>>>) -> ScopeItem {
        let mut item = ScopeItem::empty();
        item.expr = expr.into();
        item
    }

    pub fn is_from_table(&self, table_name: &PartialName) -> bool {
        match &self.table_name {
            None => false,
            Some(n) => n.matches(table_name),
        }
    }
}

impl Scope {
    pub fn empty(outer_scope: Option<Scope>) -> Self {
        Scope {
            items: vec![],
            outer_scope: outer_scope.map(Box::new),
            lateral_barrier: false,
        }
    }

    pub fn from_source<I, N>(
        table_name: Option<PartialName>,
        column_names: I,
        outer_scope: Option<Scope>,
    ) -> Self
    where
        I: IntoIterator<Item = Option<N>>,
        N: Into<ColumnName>,
    {
        let mut scope = Scope::empty(outer_scope);
        scope.items = column_names
            .into_iter()
            .map(|column_name| {
                ScopeItem::from_name(table_name.clone(), column_name.map(|n| n.into()))
            })
            .collect();
        scope
    }

    /// Constructs an iterator over the canonical name for each column.
    pub fn column_names(&self) -> impl Iterator<Item = Option<&ColumnName>> {
        self.items.iter().map(|item| item.column_name.as_ref())
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn all_items(&self) -> impl Iterator<Item = (ColumnRef, &ScopeItem)> {
        // These are in order of preference eg
        // given scopes A(B(C))
        // items from C should be preferred to items from B to items from A
        let mut items = vec![];
        let mut level = 0;
        let mut scope = self;
        loop {
            for (column, item) in scope.items.iter().enumerate() {
                items.push((ColumnRef { level, column }, item));
            }
            if let Some(outer_scope) = &scope.outer_scope {
                scope = outer_scope;
                level += 1;
            } else {
                break;
            }
        }
        items.into_iter()
    }

    /// Returns all items from the given table name in the closest scope.
    ///
    /// If no tables with the given name are in scope, returns an empty
    /// iterator.
    ///
    /// NOTE(benesch): This is wrong for zero-arity relations, because we can't
    /// distinguish between "no such table" and a table that exists but has no
    /// columns. The current design of scope makes this difficult to fix,
    /// unfortunately.
    pub fn items_from_table(&self, table: &PartialName) -> Vec<(ColumnRef, &ScopeItem)> {
        let mut seen_level = None;
        self.all_items()
            .filter(move |(column, item)| {
                item.is_from_table(table) && *seen_level.get_or_insert(column.level) == column.level
            })
            .collect()
    }

    /// Marks all scope items unnameable until the first lateral barrier is
    /// encountered.
    ///
    /// This is useful for mutating the scope before planning a non-lateral
    /// subquery.
    pub fn hide_lateral_items(&mut self) {
        let mut scope = self;
        loop {
            for item in &mut scope.items {
                item.column_name = None;
            }
            match &mut scope.outer_scope {
                Some(s) if !s.lateral_barrier => scope = s,
                _ => break,
            }
        }
    }

    fn resolve_internal<'a, M>(
        &'a self,
        mut matches: M,
        table_name: Option<&PartialName>,
        column_name: &ColumnName,
    ) -> Result<ColumnRef, PlanError>
    where
        M: FnMut(ColumnRef, &ScopeItem) -> bool,
    {
        let mut results = self
            .all_items()
            .filter(|(column, item)| (matches)(*column, item));
        match results.next() {
            None => Err(PlanError::UnknownColumn {
                table: table_name.cloned(),
                column: column_name.clone(),
            }),
            Some((column, item)) => {
                if results
                    .find(|(column2, _item)| column.level == column2.level)
                    .is_some()
                {
                    return Err(PlanError::AmbiguousColumn(column_name.clone()));
                }

                if item.lateral_error_if_referenced {
                    return Err(PlanError::WrongJoinTypeForLateralColumn {
                        table: table_name.cloned(),
                        column: column_name.clone(),
                    });
                }

                Ok(column)
            }
        }
    }

    /// Resolves references to a column name to a single column, or errors if
    /// multiple columns are equally valid references.
    pub fn resolve_column<'a>(&'a self, column_name: &ColumnName) -> Result<ColumnRef, PlanError> {
        let table_name = None;
        self.resolve_internal(
            |_column, item| {
                item.allow_unqualified_references && item.column_name.as_ref() == Some(column_name)
            },
            table_name,
            column_name,
        )
    }

    /// Resolves a column name in a `USING` clause.
    pub fn resolve_using_column(
        &self,
        column_name: &ColumnName,
        join_side: JoinSide,
    ) -> Result<ColumnRef, PlanError> {
        match self.resolve_column(column_name) {
            // We found a column in level 0, which is acceptable.
            Ok(column) if column.level == 0 => Ok(column),
            // Columns in outer scopes are not valid in USING clauses.
            Ok(_) => Err(PlanError::UnknownColumnInUsingClause {
                column: column_name.clone(),
                join_side,
            }),
            // Attach a bit more context to unknown and ambiguous column errors
            // to match PostgreSQL.
            Err(PlanError::AmbiguousColumn(column)) => {
                Err(PlanError::AmbiguousColumnInUsingClause { column, join_side })
            }
            Err(PlanError::UnknownColumn { column, .. }) => {
                Err(PlanError::UnknownColumnInUsingClause { column, join_side })
            }
            // Other errors are untouched.
            Err(e) => Err(e),
        }
    }

    pub fn resolve_table_column<'a>(
        &'a self,
        table_name: &PartialName,
        column_name: &ColumnName,
    ) -> Result<ColumnRef, PlanError> {
        let mut seen_at_level = None;
        self.resolve_internal(
            |column, item| {
                // Once we've matched a table name at a level, even if the
                // column name did not match, we can never match an item from
                // another level.
                if let Some(seen_at_level) = seen_at_level {
                    if seen_at_level != column.level {
                        return false;
                    }
                }
                if item.table_name.as_ref().map(|n| n.matches(table_name)) == Some(true) {
                    seen_at_level = Some(column.level);
                    item.column_name.as_ref() == Some(column_name)
                } else {
                    false
                }
            },
            Some(table_name),
            column_name,
        )
    }

    pub fn resolve<'a>(
        &'a self,
        table_name: Option<&PartialName>,
        column_name: &ColumnName,
    ) -> Result<ColumnRef, PlanError> {
        match table_name {
            None => self.resolve_column(column_name),
            Some(table_name) => self.resolve_table_column(table_name, column_name),
        }
    }

    /// Look to see if there is an already-calculated instance of this expr.
    /// Failing to find one is not an error, so this just returns Option
    pub fn resolve_expr<'a>(&'a self, expr: &sql_parser::ast::Expr<Aug>) -> Option<ColumnRef> {
        self.items
            .iter()
            .enumerate()
            .find(|(_, item)| item.expr.as_ref() == Some(expr))
            .map(|(i, _)| ColumnRef {
                level: 0,
                column: i,
            })
    }

    pub fn product(self, right: Self) -> Result<Self, anyhow::Error> {
        let mut l_tables = self.table_names().into_iter().collect::<Vec<_>>();
        // Make ordering deterministic for testing
        l_tables.sort_by(|l, r| l.item.cmp(&r.item));
        let r_tables = right.table_names();
        for l in l_tables {
            for r in &r_tables {
                if l.matches(r) {
                    bail!("table name \"{}\" specified more than once", l.item)
                }
            }
        }
        Ok(Scope {
            items: self
                .items
                .into_iter()
                .chain(right.items.into_iter())
                .collect(),
            outer_scope: self.outer_scope,
            lateral_barrier: false,
        })
    }

    pub fn project(&self, columns: &[usize]) -> Self {
        Scope {
            items: columns.iter().map(|&i| self.items[i].clone()).collect(),
            outer_scope: self.outer_scope.clone(),
            lateral_barrier: false,
        }
    }

    fn table_names(&self) -> HashSet<&PartialName> {
        self.items
            .iter()
            .filter_map(|name| name.table_name.as_ref())
            .collect::<HashSet<&PartialName>>()
    }
}
