// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::NaiveDate;
use serde::de::Error;
use serde::de::IntoDeserializer;
use serde::de::MapAccess;
use serde::de::Unexpected;
use serde::de::Visitor;
use serde::forward_to_deserialize_any;
use serde::Deserializer;

use crate::decode::decode_double;
use crate::decode::decode_float;
use crate::decode::decode_len;
use crate::decode::decode_long_nonneg;
use crate::error::Error as AvroError;
use crate::schema::RecordField;
use crate::schema::SchemaKind;
use crate::schema::SchemaNode;
use crate::schema::SchemaPiece;
use crate::schema::UnionSchema;
use crate::util::zag_i32;
use crate::util::zag_i64;

use std::fmt;
use std::io::Read;

#[derive(Debug)]
pub struct AvroDeError(String);

impl Error for AvroDeError {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Self(msg.to_string())
    }
}

impl std::fmt::Display for AvroDeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for AvroDeError {}

impl From<std::io::Error> for AvroDeError {
    fn from(e: std::io::Error) -> Self {
        Self::custom(format!("{:?}", e))
    }
}

impl From<AvroError> for AvroDeError {
    fn from(e: AvroError) -> Self {
        match e {
            AvroError::Decode(d_e) => Self::custom(format!("Decode error: {}", d_e)),
            AvroError::ParseSchema(_) => unreachable!(),
            AvroError::ResolveSchema(_) => unreachable!(),
            AvroError::IO(io_e) => Self::custom(format!("IO error of kind: {:?}", io_e)),
            AvroError::Allocation { attempted, allowed } => Self::custom(format!(
                "Attempted to allocate {} bytes; max allowed {}",
                attempted, allowed
            )),
        }
    }
}

impl From<std::str::Utf8Error> for AvroDeError {
    fn from(_e: std::str::Utf8Error) -> Self {
        Self::custom("Failed to decode utf-8")
    }
}

struct AvroSerdeDeserializer<'de, 'a> {
    schema: SchemaNode<'de>,
    buf: &'a mut &'de [u8],
}

macro_rules! check_union {
    ($(($func:ident, $($kind:path),*),)*) => {
	$(fn $func<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
	    where V: serde::de::Visitor<'de>
	  {
	      if self.try_step_union()? {
		  match SchemaKind::from(self.schema.inner) {
		      $($kind)|* => self.deserialize_any(visitor),
		      sk => Err(AvroDeError::custom(format!(
			  "Unexpected kind in union: {:?}",
			  sk
		      )))
		  }
	      } else {
		  self.deserialize_any(visitor)
	      }
	  }
	)*
    };
}

fn read_lengthed<'de, 'a>(buf: &'a mut &'de [u8]) -> Result<&'de [u8], AvroDeError> {
    let len = decode_len(buf)?;
    if buf.len() <= len {
        let (this_field, remaining) = buf.split_at(len);
        *buf = remaining;
        Ok(this_field)
    } else {
        Err(AvroDeError::custom(format!(
            "Length {} greater than remaining length {} of buffer",
            len,
            buf.len(),
        )))
    }
}

impl<'de, 'a> AvroSerdeDeserializer<'de, 'a> {
    fn try_step_union(&mut self) -> Result<bool, AvroDeError> {
        if let SchemaPiece::Union(inner) = self.schema.inner {
            let index = decode_long_nonneg(self.buf)? as usize;
            let variants = inner.variants();
            match variants.get(index) {
                Some(variant) => {
                    self.schema = self.schema.step(variant);
                    Ok(true)
                }
                None => Err(AvroDeError::custom(format!(
                    "Bad union index: {} (valid: [0, {}))",
                    index,
                    variants.len()
                ))),
            }
        } else {
            Ok(false)
        }
    }
}

impl<'de, 'a> Deserializer<'de> for AvroSerdeDeserializer<'de, 'a> {
    type Error = AvroDeError;

    fn deserialize_any<V>(mut self, v: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.schema.inner {
            SchemaPiece::Null => v.visit_none(),
            SchemaPiece::Boolean => {
                let mut buf = [0u8; 1];
                let () = self.buf.read_exact(&mut buf[..])?;
                let val = match buf[0] {
                    0 => false,
                    1 => true,
                    other => {
                        return Err(AvroDeError::invalid_value(
                            Unexpected::Other(&format!("Invalid boolean: {}", other)),
                            &"0 or 1",
                        ))
                    }
                };
                v.visit_bool(val)
            }
            SchemaPiece::Int => {
                let val = zag_i32(self.buf)?;
                v.visit_i32(val)
            }
            SchemaPiece::Long => {
                let val = zag_i64(self.buf)?;
                v.visit_i64(val)
            }
            SchemaPiece::Float => {
                let val = decode_float(self.buf)?;
                v.visit_f32(val)
            }
            SchemaPiece::Double => {
                let val = decode_double(self.buf)?;
                v.visit_f64(val)
            }
            SchemaPiece::Date => {
                // TODO - This does a round-trip to a string representation,
                // because that's what NaiveDate wants to deserialize from.
                //
                // We should create a wrapper around NaiveDate that lets you deserialize
                // from an int, and use that representation if we got here from
                // deserialize_i32.
                let days = zag_i32(&mut self.buf)?;
                let date = NaiveDate::from_ymd(1970, 1, 1)
                    .checked_add_signed(chrono::Duration::days(days.into()))
                    .ok_or_else(|| {
                        AvroDeError::invalid_value(
                            Unexpected::Signed(days.into()),
                            &"A valid number of days since the Unix epoch",
                        )
                    })?;

                let s = format!("{}", date);
                v.visit_string(s)
            }
            SchemaPiece::TimestampMilli => todo!(),
            SchemaPiece::TimestampMicro => todo!(),
            SchemaPiece::Decimal {
                precision,
                scale,
                fixed_size,
            } => todo!(),
            SchemaPiece::Bytes => {
                let bytes = read_lengthed(self.buf)?;
                v.visit_borrowed_bytes(bytes)
            }
            SchemaPiece::String => {
                let s = std::str::from_utf8(read_lengthed(self.buf)?)?;
                v.visit_borrowed_str(s)
            }
            SchemaPiece::Json => todo!(),
            SchemaPiece::Uuid => todo!(),
            SchemaPiece::Array(_) => todo!(),
            SchemaPiece::Map(_) => todo!(),
            SchemaPiece::Union(_) => todo!(),
            SchemaPiece::ResolveIntTsMilli => todo!(),
            SchemaPiece::ResolveIntTsMicro => todo!(),
            SchemaPiece::ResolveDateTimestamp => todo!(),
            SchemaPiece::ResolveIntLong => todo!(),
            SchemaPiece::ResolveIntFloat => todo!(),
            SchemaPiece::ResolveIntDouble => todo!(),
            SchemaPiece::ResolveLongFloat => todo!(),
            SchemaPiece::ResolveLongDouble => todo!(),
            SchemaPiece::ResolveFloatDouble => todo!(),
            SchemaPiece::ResolveConcreteUnion {
                index,
                inner,
                n_reader_variants,
                reader_null_variant,
            } => todo!(),
            SchemaPiece::ResolveUnionUnion {
                permutation,
                n_reader_variants,
                reader_null_variant,
            } => todo!(),
            SchemaPiece::ResolveUnionConcrete { index, inner } => todo!(),
            SchemaPiece::Record { fields, .. } => {
                // TODO - skip undemanded fields
                struct StructDeserializer<'a, 'b> {
                    schema: SchemaNode<'a>,
                    fields: &'a [RecordField],
                    buf: &'b mut &'a [u8],
                }
                impl<'a, 'b> MapAccess<'a> for StructDeserializer<'a, 'b> {
                    type Error = AvroDeError;

                    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
                    where
                        K: serde::de::DeserializeSeed<'a>,
                    {
                        let answer = match self.fields.first() {
                            Some(field) => Some(seed.deserialize(<&str as IntoDeserializer<
                                AvroDeError,
                            >>::into_deserializer(
                                field.name.as_str()
                            ))?),
                            None => None,
                        };
                        Ok(answer)
                    }

                    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
                    where
                        V: serde::de::DeserializeSeed<'a>,
                    {
                        // Intentionally panic here if fields is empty,
                        // because it means the visitor is calling things in the wrong
                        // order. Otherwise, non-emptiness of `self.fields` would have been
                        // checked in `next_key_seed` above.
                        let (f, fs) = self.fields.split_at(1);
                        self.fields = fs;
                        let next_schema = self.schema.step(&f[0].schema);
                        let result = seed.deserialize(AvroSerdeDeserializer {
                            schema: next_schema,
                            buf: self.buf,
                        });
                        result
                    }
                }

                v.visit_map(StructDeserializer {
                    schema: self.schema,
                    fields,
                    buf: self.buf,
                })
            }
            SchemaPiece::Enum {
                doc,
                symbols,
                default_idx,
            } => todo!(),
            SchemaPiece::Fixed { size } => todo!(),
            SchemaPiece::ResolveRecord {
                defaults,
                fields,
                n_reader_fields,
            } => todo!(),
            SchemaPiece::ResolveEnum {
                doc,
                symbols,
                default,
            } => todo!(),
        }
    }

    fn deserialize_struct<V>(
        mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.try_step_union()?;
        if self
            .schema
            .name
            .map(|full_name| full_name.base_name() == name)
            .unwrap_or(false)
        {
            self.deserialize_any(visitor)
        } else {
            Err(AvroDeError::custom(format!(
                "Mismatched name: tried to decode struct {} against schema node {}",
                name,
                self.schema
                    .name
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "(None)".to_string())
            )))
        }
    }

    check_union! {
        (deserialize_str, SchemaKind::String),
        (deserialize_i128, SchemaKind::Long, SchemaKind::Int),
        (deserialize_i64, SchemaKind::Long, SchemaKind::Int),
        (deserialize_i32, SchemaKind::Int),
        (deserialize_f64, SchemaKind::Double, SchemaKind::Float),
        (deserialize_f32, SchemaKind::Float),
        (deserialize_bytes, SchemaKind::Bytes),
        (deserialize_byte_buf, SchemaKind::Bytes),
        (deserialize_string, SchemaKind::String),
    }

    forward_to_deserialize_any! {
        bool i8 i16 u8 u16 u32 u64 u128 char
        option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}
