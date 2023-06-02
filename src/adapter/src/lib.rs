// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG
// Disallow usage of `unwrap()`.
#![warn(clippy::unwrap_used)]
#![cfg_attr(nightly_doc_features, feature(doc_cfg))]
// Without this, cargo clippy complains with:
//     overflow evaluating the requirement `&str: std::marker::Send`
// in implement_peek_plan's return of a Result<crate::ExecuteResponse, AdapterError>.
#![recursion_limit = "256"]

//! Coordinates client requests with the dataflow layer.
//!
//! This crate hosts the "coordinator", an object which sits at the center of
//! the system and coordinates communication between the various components.
//! Responsibilities of the coordinator include:
//!
//!   * Launching the dataflow workers.
//!   * Periodically allowing the dataflow workers to compact existing data.
//!   * Executing SQL queries from clients by parsing and planning them, sending
//!     the plans to the dataflow layer, and then streaming the results back to
//!     the client.
//!   * Assigning timestamps to incoming source data.
//!
//! The main interface to the coordinator is [`Client`]. To start a coordinator,
//! use the [`serve`] function.

// TODO(benesch): delete this once we use structured errors everywhere.
macro_rules! coord_bail {
    ($($e:expr),*) => {
        return Err(crate::error::AdapterError::Unstructured(::anyhow::anyhow!($($e),*)))
    }
}

mod command;
mod coord;
mod error;
mod explain;
mod notice;
mod rbac;
mod subscribe;
mod util;

pub mod catalog;
pub mod client;
pub mod config;
pub mod metrics;
pub mod session;
pub mod telemetry;

pub(crate) use crate::coord::statement_logging;

pub use crate::client::{Client, Handle, SessionClient};
pub use crate::command::{
    Canceled, ExecuteResponse, ExecuteResponseKind, RowsFuture, StartupMessage, StartupResponse,
};
pub use crate::coord::id_bundle::CollectionIdBundle;
pub use crate::coord::peek::PeekResponseUnary;
pub use crate::coord::timeline::TimelineContext;
pub use crate::coord::timestamp_selection::{
    TimestampContext, TimestampExplanation, TimestampProvider,
};
pub use crate::coord::ExecuteContext;
pub use crate::coord::ExecuteContextExtra;
pub use crate::coord::{serve, Config, DUMMY_AVAILABILITY_ZONE};
pub use crate::error::AdapterError;
pub use crate::notice::AdapterNotice;
