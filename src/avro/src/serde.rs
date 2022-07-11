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

use crate::{decode::decode_boolean, schema::SchemaNode};

pub struct AvroSerdeDeserializer<'schema, 'input> {
    s: SchemaNode<'schema>,
    r: &'input [u8],
}

impl<'schema, 'de> serde::Deserializer<'de> for AvroSerdeDeserializer<'schema, 'de> {
    type Error = crate::error::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // TODO - "strict mode" that only lets this be a bool
        match self.s.inner {
            crate::schema::SchemaPiece::Boolean => {
                let v = decode_boolean(&mut self.r)?;
                visitor.visit_bool(v)
            }
            // crate::schema::SchemaPiece::Int => todo!(),
            // crate::schema::SchemaPiece::Long => todo!(),
            // crate::schema::SchemaPiece::Float => todo!(),
            // crate::schema::SchemaPiece::Double => todo!(),
            // crate::schema::SchemaPiece::Date => todo!(),
            // crate::schema::SchemaPiece::TimestampMilli => todo!(),
            // crate::schema::SchemaPiece::TimestampMicro => todo!(),
            // crate::schema::SchemaPiece::Decimal { precision, scale, fixed_size } => todo!(),
            // crate::schema::SchemaPiece::Bytes => todo!(),
            // crate::schema::SchemaPiece::String => todo!(),
            // crate::schema::SchemaPiece::Json => todo!(),
            // crate::schema::SchemaPiece::Uuid => todo!(),
            // crate::schema::SchemaPiece::Array(_) => todo!(),
            // crate::schema::SchemaPiece::Map(_) => todo!(),
            crate::schema::SchemaPiece::Union(us) => {
                 
            }
            crate::schema::SchemaPiece::ResolveIntTsMilli
            | crate::schema::SchemaPiece::ResolveIntTsMicro
            | crate::schema::SchemaPiece::ResolveDateTimestamp
            | crate::schema::SchemaPiece::ResolveIntLong
            | crate::schema::SchemaPiece::ResolveIntFloat
            | crate::schema::SchemaPiece::ResolveIntDouble
            | crate::schema::SchemaPiece::ResolveLongFloat
            | crate::schema::SchemaPiece::ResolveLongDouble
            | crate::schema::SchemaPiece::ResolveFloatDouble
            | crate::schema::SchemaPiece::ResolveConcreteUnion { .. }
            | crate::schema::SchemaPiece::ResolveUnionUnion { .. }
            | crate::schema::SchemaPiece::ResolveUnionConcrete { index, inner } => {
                panic!("Attempted to deserialize with Serde from a resolved schema") // TODO - link to documentation about why this is bad
            }
            crate::schema::SchemaPiece::Record {
                doc,
                fields,
                lookup,
            } => todo!(),
            crate::schema::SchemaPiece::Enum {
                doc,
                symbols,
                default_idx,
            } => todo!(),
            crate::schema::SchemaPiece::Fixed { size } => todo!(),
            crate::schema::SchemaPiece::ResolveRecord {
                defaults,
                fields,
                n_reader_fields,
            } => todo!(),
            crate::schema::SchemaPiece::ResolveEnum {
                doc,
                symbols,
                default,
            } => todo!(),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}
