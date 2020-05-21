// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::mem::transmute;

use chrono::{NaiveDate, NaiveDateTime};
use failure::{bail, Error};

use crate::from_avro_datum;
use crate::schema::{
    RecordField, ResolvedRecordField, SchemaNode, SchemaPiece, SchemaPieceOrNamed, UnionSchema,
};
use crate::types::{DecimalValue, Scalar, Value};
use crate::util::{safe_len, zag_i32, zag_i64, DecodeError};
use std::{
    io::{Read, SeekFrom},
    marker::PhantomData,
};

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(Value::Long)
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i32(reader).map(Value::Int)
}

#[inline]
fn decode_long_nonneg<R: Read>(reader: &mut R) -> Result<u64, Error> {
    let u = match zag_i64(reader)? {
        i if i >= 0 => i as u64,
        i => bail!("Expected non-negative integer, got {}", i),
    };
    Ok(u)
}

fn decode_int_nonneg<R: Read>(reader: &mut R) -> Result<u32, Error> {
    let u = match zag_i32(reader)? {
        i if i >= 0 => i as u32,
        i => bail!("Expected non-negative integer, got {}", i),
    };
    Ok(u)
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|i| safe_len(i as usize))
}

fn decode_decimal<R: Read>(
    reader: &mut R,
    precision: usize,
    scale: usize,
    fixed_size: Option<usize>,
) -> Result<DecimalValue, Error> {
    let len = match fixed_size {
        Some(len) => len,
        None => decode_len(reader)?,
    };
    let mut buf = Vec::with_capacity(len);
    // FIXME(brennan) - this is UB iff `reader`
    // looks at the bytes. Nuke it during the avro decod3
    // refactor.
    unsafe {
        buf.set_len(len);
    }
    reader.read_exact(&mut buf)?;
    Ok(DecimalValue {
        unscaled: buf,
        precision,
        scale,
    })
}

#[inline]
fn decode_float<R: Read>(reader: &mut R) -> Result<f32, Error> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf[..])?;
    Ok(unsafe { transmute::<[u8; 4], f32>(buf) })
}

#[inline]
fn decode_double<R: Read>(reader: &mut R) -> Result<f64, Error> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf[..])?;
    Ok(unsafe { transmute::<[u8; 8], f64>(buf) })
}

fn decode_string<R: Read>(reader: &mut R) -> Result<String, Error> {
    let len = decode_len(reader)?;
    let mut buf = Vec::with_capacity(len);
    unsafe {
        buf.set_len(len);
    }
    reader.read_exact(&mut buf)?;

    String::from_utf8(buf).map_err(|_| DecodeError::new("not a valid utf-8 string").into())
}

pub trait Skip {
    fn skip(&mut self, len: usize) -> Result<(), Error>;
}

mod private {
    // Impl this for any Seek types that we want to make Skip.
    pub trait SkipMarker {}
}

impl private::SkipMarker for std::fs::File {}

impl<T> Skip for T
where
    T: std::io::Seek + private::SkipMarker,
{
    fn skip(&mut self, len: usize) -> Result<(), Error> {
        self.seek(SeekFrom::Current(len as i64))?;
        Ok(())
    }
}

impl Skip for &[u8] {
    fn skip(&mut self, len: usize) -> Result<(), Error> {
        if len > self.len() {
            bail!(
                "Failed to skip {} bytes in buffer: length is {}.",
                len,
                self.len()
            );
        }
        *self = &self[len..];
        Ok(())
    }
}

pub enum ValueOrReader<'a, V, R: Read + Skip> {
    Value(V),
    Reader { len: usize, r: &'a mut R },
}

pub trait AvroDecode {
    fn begin_record(&mut self) -> Result<(), Error>;
    fn record_field(&mut self, name: &str, position: usize) -> Result<(), Error>;
    fn end_record(&mut self) -> Result<(), Error>;

    fn union_branch(&mut self, idx: usize) -> Result<(), Error>;

    fn begin_array(&mut self) -> Result<(), Error>;
    fn end_array(&mut self) -> Result<(), Error>;

    fn begin_map(&mut self) -> Result<(), Error>;
    fn map_key<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a str, R>,
    ) -> Result<(), Error>;
    fn end_map(&mut self) -> Result<(), Error>;

    fn enum_variant(&mut self, symbol: &str, idx: usize) -> Result<(), Error>;

    fn scalar(&mut self, scalar: Scalar) -> Result<(), Error>;

    fn decimal<'a, R: Read + Skip>(
        &mut self,
        precision: usize,
        scale: usize,
        r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error>;
    fn bytes<'a, R: Read + Skip>(&mut self, r: ValueOrReader<'a, &'a [u8], R>)
        -> Result<(), Error>;
    fn string<'a, R: Read + Skip>(&mut self, r: ValueOrReader<'a, &'a str, R>)
        -> Result<(), Error>;
    fn json<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a serde_json::Value, R>,
    ) -> Result<(), Error>;
    fn fixed<'a, R: Read + Skip>(&mut self, r: ValueOrReader<'a, &'a [u8], R>)
        -> Result<(), Error>;
}

struct TrivialDecoder;

impl TrivialDecoder {
    fn maybe_skip<'a, V, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, V, R>,
    ) -> Result<(), Error> {
        if let ValueOrReader::Reader { len, r } = r {
            r.skip(len)
        } else {
            Ok(())
        }
    }
}

impl AvroDecode for TrivialDecoder {
    fn begin_record(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn record_field(&mut self, name: &str, position: usize) -> Result<(), Error> {
        Ok(())
    }
    fn end_record(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn union_branch(&mut self, idx: usize) -> Result<(), Error> {
        Ok(())
    }
    fn begin_array(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn end_array(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn begin_map(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn map_key<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a str, R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn end_map(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn enum_variant(&mut self, symbol: &str, idx: usize) -> Result<(), Error> {
        Ok(())
    }
    fn scalar(&mut self, scalar: Scalar) -> Result<(), Error> {
        Ok(())
    }
    fn decimal<'a, R: Read + Skip>(
        &mut self,
        precision: usize,
        scale: usize,
        r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn bytes<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn string<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a str, R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn json<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a serde_json::Value, R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
    fn fixed<'a, R: Read + Skip>(
        &mut self,
        r: ValueOrReader<'a, &'a [u8], R>,
    ) -> Result<(), Error> {
        self.maybe_skip(r)
    }
}

fn give_value<D: AvroDecode>(d: &mut D, v: &Value) -> Result<(), Error> {
    use ValueOrReader::Value as V;
    match v {
        Value::Null => d.scalar(Scalar::Null),
        Value::Boolean(val) => d.scalar(Scalar::Boolean(*val)),
        Value::Int(val) => d.scalar(Scalar::Int(*val)),
        Value::Long(val) => d.scalar(Scalar::Long(*val)),
        Value::Float(val) => d.scalar(Scalar::Float(*val)),
        Value::Double(val) => d.scalar(Scalar::Double(*val)),
        Value::Date(val) => d.scalar(Scalar::Date(*val)),
        Value::Timestamp(val) => d.scalar(Scalar::Timestamp(*val)),
        // The &[u8] parameter here (and elsewhere in this function) is arbitrary, but we have to put in something in order for the function
        // to type-check
        Value::Decimal(val) => d.decimal::<&[u8]>(val.precision, val.scale, V(&val.unscaled)),
        Value::Bytes(val) => d.bytes::<&[u8]>(V(val)),
        Value::String(val) => d.string::<&[u8]>(V(val)),
        Value::Fixed(_len, val) => d.fixed::<&[u8]>(V(val)),
        Value::Enum(idx, symbol) => d.enum_variant(symbol, *idx as usize),
        Value::Union(idx, val) => {
            d.union_branch(*idx)?;
            give_value(d, val.as_ref())
        }
        Value::Array(val) => {
            d.begin_array()?;
            for v in val {
                give_value(d, v)?;
            }
            d.end_array()
        }
        Value::Map(val) => {
            d.begin_map()?;
            for (key, v) in val {
                d.map_key::<&[u8]>(V(key))?;
                give_value(d, v)?;
            }
            d.end_map()
        }
        Value::Record(val) => {
            d.begin_record()?;
            for (i, (name, v)) in val.iter().enumerate() {
                d.record_field(name, i)?;
                give_value(d, v)?;
            }
            d.end_record()
        }
        Value::Json(val) => d.json::<&[u8]>(V(val)),
    }
}

pub fn decode_new<'a, R: Read + Skip, D: AvroDecode>(
    schema: SchemaNode<'a>,
    r: &'a mut R,
    d: &'a mut D,
) -> Result<(), Error> {
    use ValueOrReader::Reader;
    match schema.inner {
        SchemaPiece::Null => d.scalar(Scalar::Null),
        SchemaPiece::Boolean => {
            let mut buf = [0u8; 1];
            r.read_exact(&mut buf[..])?;
            let val = match buf[0] {
                0u8 => false,
                1u8 => true,
                _ => return Err(DecodeError::new("not a bool").into()),
            };
            d.scalar(Scalar::Boolean(val))
        }
        SchemaPiece::Int => {
            let val = zag_i32(r)?;
            d.scalar(Scalar::Int(val))
        }
        SchemaPiece::Long => {
            let val = zag_i64(r)?;
            d.scalar(Scalar::Long(val))
        }
        SchemaPiece::Float => {
            let val = decode_float(r)?;
            d.scalar(Scalar::Float(val))
        }
        SchemaPiece::Double => {
            let val = decode_double(r)?;
            d.scalar(Scalar::Double(val))
        }
        SchemaPiece::Date => {
            let days = zag_i32(r)?;
            let val = NaiveDate::from_ymd(1970, 1, 1)
                .checked_add_signed(chrono::Duration::days(days.into()))
                .ok_or_else(|| {
                    DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                })?;
            d.scalar(Scalar::Date(val))
        }
        SchemaPiece::TimestampMilli => {
            let total_millis = zag_i32(r)?;
            let seconds = total_millis / 1000;
            let millis = (total_millis % 1000) as u32;
            let val = NaiveDateTime::from_timestamp_opt(seconds as i64, millis * 1_000_000)
                .ok_or_else(|| {
                    DecodeError::new(format!("Invalid ms timestamp {}.{}", seconds, millis))
                })?;
            d.scalar(Scalar::Timestamp(val))
        }
        SchemaPiece::TimestampMicro => {
            let total_micros = zag_i64(r)?;
            let seconds = total_micros / 1_000_000;
            let micros = (total_micros % 1_000_000) as u32;
            let val =
                NaiveDateTime::from_timestamp_opt(seconds, micros * 1000).ok_or_else(|| {
                    DecodeError::new(format!(
                        "Invalid microsecond timestamp: {}.{}",
                        seconds, micros
                    ))
                })?;
            d.scalar(Scalar::Timestamp(val))
        }
        SchemaPiece::Decimal {
            precision,
            scale,
            fixed_size,
        } => {
            let len = fixed_size.map(Ok).unwrap_or_else(|| decode_len(r))?;
            d.decimal(*precision, *scale, Reader { len, r })
        }
        SchemaPiece::Bytes => {
            let len = decode_len(r)?;
            d.bytes(Reader { len, r })
        }
        SchemaPiece::String => {
            let len = decode_len(r)?;
            d.string(Reader { len, r })
        }
        SchemaPiece::Json => {
            let len = decode_len(r)?;
            d.json(Reader { len, r })
        }
        SchemaPiece::Array(inner) => {
            // From the spec:
            // Arrays are encoded as a series of blocks. Each block consists of a long count value, followed by that many array items. A block with count zero indicates the end of the array. Each item is encoded per the array's item schema.
            // If a block's count is negative, its absolute value is used, and the count is followed immediately by a long block size indicating the number of bytes in the block. This block size permits fast skipping through data, e.g., when projecting a record to a subset of its fields.

            d.begin_array()?;
            loop {
                // TODO -- we can use len_in_bytes to quickly skip non-demanded arrays
                let (len, _len_in_bytes) = match zag_i64(r)? {
                    len if len > 0 => (len as usize, None),
                    neglen if neglen < 0 => (neglen.abs() as usize, Some(decode_len(r)?)),
                    0 => break,
                    _ => unreachable!(),
                };

                for _ in 0..len {
                    decode_new(schema.step(inner.as_ref()), r, d)?;
                }
            }
            d.end_array()
        }
        SchemaPiece::Map(inner) => {
            // See logic for `SchemaPiece::Array` above. Maps are encoded similarly.
            d.begin_map()?;
            loop {
                let (len, _len_in_bytes) = match zag_i64(r)? {
                    len if len > 0 => (len as usize, None),
                    neglen if neglen < 0 => (neglen.abs() as usize, Some(decode_len(r)?)),
                    0 => break,
                    _ => unreachable!(),
                };

                for _ in 0..len {
                    let key_len = decode_len(r)?;
                    d.map_key(Reader { len: key_len, r })?;
                    decode_new(schema.step(inner.as_ref()), r, d)?;
                }
            }
            d.end_map()
        }
        SchemaPiece::Union(inner) => {
            let index = decode_long_nonneg(r)? as usize;
            let variants = inner.variants();
            match variants.get(index) {
                Some(variant) => {
                    d.union_branch(index)?;
                    decode_new(schema.step(variant), r, d)
                }
                None => Err(DecodeError::new("Union index out of bounds").into()),
            }
        }
        SchemaPiece::ResolveIntLong => {
            let val = zag_i32(r)? as i64;
            d.scalar(Scalar::Long(val))
        }
        SchemaPiece::ResolveIntFloat => {
            let val = zag_i32(r)? as f32;
            d.scalar(Scalar::Float(val))
        }
        SchemaPiece::ResolveIntDouble => {
            let val = zag_i32(r)? as f64;
            d.scalar(Scalar::Double(val))
        }
        SchemaPiece::ResolveLongFloat => {
            let val = zag_i64(r)? as f32;
            d.scalar(Scalar::Float(val))
        }
        SchemaPiece::ResolveLongDouble => {
            let val = zag_i64(r)? as f64;
            d.scalar(Scalar::Double(val))
        }
        SchemaPiece::ResolveFloatDouble => {
            let val = decode_float(r)? as f64;
            d.scalar(Scalar::Double(val))
        }
        SchemaPiece::ResolveConcreteUnion { index, inner } => {
            d.union_branch(*index)?;
            decode_new(schema.step(inner.as_ref()), r, d)
        }
        SchemaPiece::ResolveUnionUnion { permutation } => {
            let index = decode_long_nonneg(r)? as usize;
            match &permutation[index] {
                None => Err(DecodeError::new("Union variant not found in reader schema").into()),
                Some((index, variant)) => {
                    d.union_branch(*index)?;
                    decode_new(schema.step(variant), r, d)
                }
            }
        }
        SchemaPiece::ResolveUnionConcrete { index, inner } => {
            let found_index = decode_long_nonneg(r)? as usize;
            if *index != found_index {
                Err(
                    DecodeError::new("Union variant does not match reader schema's concrete type")
                        .into(),
                )
            } else {
                // The reader is not expecting a union here, so don't call `D::union_branch`
                decode_new(schema.step(inner.as_ref()), r, d)
            }
        }
        SchemaPiece::Record {
            doc,
            fields,
            lookup,
        } => {
            d.begin_record()?;
            for field in fields {
                d.record_field(&field.name, field.position)?;
                decode_new(schema.step(&field.schema), r, d)?;
            }
            d.end_record()
        }
        SchemaPiece::Enum { doc, symbols } => {
            let index = decode_int_nonneg(r)? as usize;
            match symbols.get(index) {
                None => Err(DecodeError::new(format!(
                    "enum symbol index out of bounds: {}, [0, {})",
                    index,
                    symbols.len()
                ))
                .into()),
                Some(symbol) => d.enum_variant(symbol, index),
            }
        }
        SchemaPiece::Fixed { size } => d.fixed(Reader { len: *size, r }),
        // XXX - This does not deliver fields to the consumer in the same order they were
        // declared in the reader schema, which might cause headache for consumers...
        // Unfortunately, there isn't a good way to do so without pre-decoding the whole record
        // (which would require a lot of allocations)
        // and then sorting the fields. So, just let the consumer deal with re-ordering.
        SchemaPiece::ResolveRecord {
            defaults,
            fields,
            n_reader_fields,
        } => {
            for default in defaults {
                d.record_field(&default.name, default.position)?;
                give_value(d, &default.default)?;
            }
            for field in fields {
                match field {
                    ResolvedRecordField::Present(field) => {
                        d.record_field(&field.name, field.position)?;
                        decode_new(schema.step(&field.schema), r, d)?;
                    }
                    ResolvedRecordField::Absent(writer_schema) => {
                        // we don't care what's in the value, but we still need to read it in order to skip ahead the proper amount in the input.
                        decode_new(writer_schema.top_node(), r, &mut TrivialDecoder)?;
                    }
                }
            }
            d.end_record()
        }
        SchemaPiece::ResolveEnum { doc, symbols } => {
            let index = decode_int_nonneg(r)? as usize;
            match symbols.get(index) {
                None => Err(DecodeError::new(format!(
                    "enum symbol index out of bounds: {}, [0, {})",
                    index,
                    symbols.len()
                ))
                .into()),
                Some(op) => match op {
                    None => Err(DecodeError::new(format!(
                        "Enum symbol at index {} in writer schema not found in reader",
                        index
                    ))
                    .into()),
                    Some((name, index)) => d.enum_variant(name, *index),
                },
            }
        }
    }
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<'a, R: Read>(schema: SchemaNode<'a>, reader: &'a mut R) -> Result<Value, Error> {
    match schema.inner {
        SchemaPiece::Null => Ok(Value::Null),
        SchemaPiece::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false)),
                1u8 => Ok(Value::Boolean(true)),
                _ => Err(DecodeError::new("not a bool").into()),
            }
        }
        SchemaPiece::Int => decode_int(reader),
        SchemaPiece::Long => decode_long(reader),
        SchemaPiece::Float => decode_float(reader).map(Value::Float),
        SchemaPiece::Double => decode_double(reader).map(Value::Double),
        SchemaPiece::Date => match decode_int(reader)? {
            Value::Int(days) => Ok(Value::Date(
                NaiveDate::from_ymd(1970, 1, 1)
                    .checked_add_signed(chrono::Duration::days(days.into()))
                    .ok_or_else(|| {
                        DecodeError::new(format!("Invalid num days from epoch: {0}", days))
                    })?,
            )),
            other => {
                Err(DecodeError::new(format!("Not an Int32 input for Date: {:?}", other)).into())
            }
        },
        SchemaPiece::TimestampMilli => match decode_long(reader)? {
            Value::Long(millis) => {
                let seconds = millis / 1_000;
                let millis = (millis % 1_000) as u32;
                Ok(Value::Timestamp(
                    NaiveDateTime::from_timestamp_opt(seconds, millis * 1_000_000).ok_or_else(
                        || DecodeError::new(format!("Invalid ms timestamp {}.{}", seconds, millis)),
                    )?,
                ))
            }
            other => Err(DecodeError::new(format!(
                "Not an Int64 input for Millisecond DateTime: {:?}",
                other
            ))
            .into()),
        },
        SchemaPiece::TimestampMicro => match decode_long(reader)? {
            Value::Long(micros) => {
                let seconds = micros / 1_000_000;
                let micros = (micros % 1_000_000) as u32;
                Ok(Value::Timestamp(
                    NaiveDateTime::from_timestamp_opt(seconds, micros * 1_000).ok_or_else(
                        || DecodeError::new(format!("Invalid mu timestamp {}.{}", seconds, micros)),
                    )?,
                ))
            }
            other => Err(DecodeError::new(format!(
                "Not an Int64 input for Microsecond DateTime: {:?}",
                other
            ))
            .into()),
        },
        SchemaPiece::Decimal {
            precision,
            scale,
            fixed_size,
        } => {
            let decimal = decode_decimal(reader, *precision, *scale, *fixed_size)?;
            Ok(Value::Decimal(decimal))
        }
        SchemaPiece::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;
            Ok(Value::Bytes(buf))
        }
        SchemaPiece::String => decode_string(reader).map(Value::String),
        SchemaPiece::Json => {
            let s = decode_string(reader)?;
            let j = serde_json::from_str(s.as_str())?;
            Ok(Value::Json(j))
        }
        SchemaPiece::Array(inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_len(reader)?;
                // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the array
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    items.push(decode(schema.step(&**inner), reader)?);
                }
            }

            Ok(Value::Array(items))
        }
        SchemaPiece::Map(inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_len(reader)?;
                // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the map
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(key) = decode(
                        schema.step(&SchemaPieceOrNamed::Piece(SchemaPiece::String)),
                        reader,
                    )? {
                        let value = decode(schema.step(&**inner), reader)?;
                        items.insert(key, value);
                    } else {
                        return Err(DecodeError::new("map key is not a string").into());
                    }
                }
            }

            Ok(Value::Map(items))
        }
        SchemaPiece::Union(inner) => {
            let index = zag_i64(reader)? as usize;
            let variants = inner.variants();
            match variants.get(index) {
                Some(variant) => {
                    decode(schema.step(variant), reader).map(|x| Value::Union(index, Box::new(x)))
                }
                None => Err(DecodeError::new("Union index out of bounds").into()),
            }
        }
        SchemaPiece::ResolveIntLong => zag_i32(reader).map(|x| Value::Long(x.into())),
        SchemaPiece::ResolveIntFloat => zag_i32(reader).map(|x| Value::Float(x as f32)),
        SchemaPiece::ResolveIntDouble => zag_i32(reader).map(|x| Value::Double(x.into())),
        SchemaPiece::ResolveLongFloat => zag_i64(reader).map(|x| Value::Float(x as f32)),
        SchemaPiece::ResolveLongDouble => zag_i64(reader).map(|x| Value::Double(x as f64)),
        SchemaPiece::ResolveFloatDouble => decode_float(reader).map(|x| Value::Double(x as f64)),
        SchemaPiece::ResolveConcreteUnion { index, inner } => {
            decode(schema.step(&**inner), reader).map(|x| Value::Union(*index, Box::new(x)))
        }
        SchemaPiece::ResolveUnionUnion { permutation } => {
            let index = zag_i64(reader)? as usize;
            match &permutation[index] {
                None => Err(DecodeError::new("Union variant not found in reader schema").into()),
                Some((index, variant)) => {
                    decode(schema.step(variant), reader).map(|x| Value::Union(*index, Box::new(x)))
                }
            }
        }
        SchemaPiece::ResolveUnionConcrete { index, inner } => {
            let found_index = zag_i64(reader)? as usize;
            if *index != found_index {
                Err(
                    DecodeError::new("Union variant does not match reader schema's concrete type")
                        .into(),
                )
            } else {
                decode(schema.step(&**inner), reader)
            }
        }
        SchemaPiece::Record { ref fields, .. } => {
            // Benchmarks indicate ~10% improvement using this method.
            let mut items = Vec::new();
            items.reserve(fields.len());
            for field in fields {
                // This clone is also expensive. See if we can do away with it...
                items.push((
                    field.name.clone(),
                    decode(schema.step(&field.schema), reader)?,
                ));
            }
            Ok(Value::Record(items))
            // fields
            // .iter()
            // .map(|field| decode(&field.schema, reader).map(|value| (field.name.clone(), value)))
            // .collect::<Result<Vec<(String, Value)>, _>>()
            // .map(|items| Value::Record(items))
        }
        SchemaPiece::Enum { ref symbols, .. } => {
            if let Value::Int(index) = decode_int(reader)? {
                if index >= 0 && (index as usize) <= symbols.len() {
                    let symbol = symbols[index as usize].clone();
                    Ok(Value::Enum(index, symbol))
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        }
        SchemaPiece::Fixed { size, .. } => {
            let mut buf = vec![0u8; *size as usize];
            reader.read_exact(&mut buf)?;
            Ok(Value::Fixed(*size, buf))
        }
        SchemaPiece::ResolveRecord {
            defaults,
            fields,
            n_reader_fields,
        } => {
            let mut items: Vec<Option<(String, Value)>> = vec![None; *n_reader_fields];
            for default in defaults {
                assert!(default.position < items.len(), "Internal error: n_reader_fields should have been big enough to cover all default positions!");
                if items[default.position].is_some() {
                    return Err(DecodeError::new(format!(
                        "Duplicate record field: {}",
                        &default.name
                    ))
                    .into());
                }
                items[default.position] = Some((default.name.clone(), default.default.clone()));
            }
            for field in fields {
                match field {
                    ResolvedRecordField::Present(field) => {
                        assert!(field.position < items.len(), "Internal error: n_reader_fields should have been big enough to cover all field positions!");
                        if items[field.position].is_some() {
                            return Err(DecodeError::new(format!(
                                "Duplicate record field: {}",
                                &field.name
                            ))
                            .into());
                        }
                        items[field.position] = Some((
                            field.name.clone(),
                            decode(schema.step(&field.schema), reader)?,
                        ))
                    }
                    ResolvedRecordField::Absent(writer_schema) => {
                        let _ignored_val = from_avro_datum(writer_schema, reader)?;
                    }
                }
            }
            let items: Option<Vec<(String, Value)>> = items.into_iter().collect();
            match items {
                Some(items) => Ok(Value::Record(items)),
                None => Err(DecodeError::new(format!(
                    "Not all fields present in {} -- issue with schema resolution?",
                    &schema.name.unwrap()
                ))
                .into()),
            }
        }
        SchemaPiece::ResolveEnum { doc: _, symbols } => {
            if let Value::Int(index) = decode_int(reader)? {
                if index >= 0 && (index as usize) <= symbols.len() {
                    match symbols[index as usize].clone() {
                        Some((symbol, index)) => Ok(Value::Enum(index as i32, symbol)),
                        None => Err(DecodeError::new(format!(
                            "Enum symbol at index {} in writer schema not found in reader",
                            index
                        ))
                        .into()),
                    }
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        }
    }
}
