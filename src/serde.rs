use std::convert::TryFrom;
use c_serde::{
    Deserialize, 
    Serializer, 
    de::{DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor}, 
    ser::{SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant}
};
use crate::serialization::Serialize;
use crate::result::{Error, Result as SResult};

impl c_serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Unsupported(msg.to_string())
    }
}
impl c_serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg:T) -> Self {
        Error::Unsupported(msg.to_string())
    }
}

struct IVecSer {
    buf: Vec<u8>,
    offset: usize,
}

pub fn to_vec<S: c_serde::Serialize>(obj: &S) -> Vec<u8> {
    let mut ser = IVecSer {
        buf: Vec::default(),
        offset: 0,
    };
    obj.serialize(&mut ser).unwrap();
    ser.buf
}

impl IVecSer {
    fn put<T: Serialize + Copy>(&mut self, v: T) -> Result<(), Error> {
        let req_sz = usize::try_from(self.offset as u64 + v.serialized_size())
            .expect("object is too big to be serialized");
        if req_sz > self.buf.len() {
            // more space required
            self.buf.resize(req_sz, 0);
        }
        
        let mut remaining_bytes = &mut self.buf[self.offset..];
        let remaining_len = remaining_bytes.len();
        v.serialize_into(&mut remaining_bytes);

        let bytes_written = remaining_len - remaining_bytes.len();
        self.offset += bytes_written;
        Ok(())
    }

    fn put_u16(&mut self, v: u16) -> Result<(), Error> {
        self.put(u32::from(v))
    }

    fn put_bytes(&mut self, b: &[u8]) -> Result<(), Error> {
        let len = u32::try_from(b.len()).map_err(|_| Error::Unsupported("can't serialize buffer with length > u32::MAX".to_string()))?;
        self.put(len)?;
        self.buf.extend_from_slice(b);
        self.offset += b.len();
        Ok(())
    }
}

impl<'a> Serializer for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Self;

    type SerializeTupleVariant = Self;

    type SerializeMap = Self;

    type SerializeStruct = Self;

    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        self.put(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.put(u8::from_ne_bytes(v.to_ne_bytes()))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u16(u16::from_ne_bytes(v.to_ne_bytes()))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.put(u32::from_ne_bytes(v.to_ne_bytes()))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.put(u64::from_ne_bytes(v.to_ne_bytes()))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.put(v)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.put_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.put(v)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.put(v)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        let e = u32::from_ne_bytes(v.to_ne_bytes());
        self.put(e)
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let e = u64::from_ne_bytes(v.to_ne_bytes());
        self.put(e)
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.put(v as u32)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.put_bytes(v)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_bool(false)
    }

    fn serialize_some<T: ?Sized + c_serde::Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.serialize_bool(true)?;
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(self, _name: &'static str, variant_index: u32, _variant: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(variant_index)
    }

    fn serialize_newtype_struct<T: ?Sized + c_serde::Serialize>(self, _name: &'static str, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + c_serde::Serialize>(self, _name: &'static str, variant_index: u32, _variant: &'static str, value: &T) -> Result<Self::Ok, Self::Error> {
        self.serialize_u32(variant_index)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match len {
            Some(v) => self.serialize_tuple(v),
            None => Err(Error::Unsupported("serialized sequence size must be know".to_string()))
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        let l32 = u32::try_from(len)
            .map_err(|_| Error::Unsupported("tuple is too big to serialize".to_string()))?;

        self.serialize_u32(l32)?;
        Ok(self)
    }

    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(self, _name: &'static str, variant_index: u32, _variant: &'static str, len: usize) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_u32(variant_index)?;
        self.serialize_tuple(len)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match len {
            Some(len) => {
                let l32 = u32::try_from(len)
                    .map_err(|_| Error::Unsupported("map is too big to serialize".to_string()))?;
                self.serialize_u32(l32)?
            },
            None => return Err(Error::Unsupported("map length must be known".to_string()))
        }
        
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_tuple(len)
    }

    fn serialize_struct_variant(self, name: &'static str, variant_index: u32, _variant: &'static str, len: usize) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.serialize_u32(variant_index)?;
        self.serialize_struct(name, len)
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a> SerializeTuple for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T: ?Sized + c_serde::Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeTupleVariant for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T: c_serde::Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeSeq for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T: ?Sized + c_serde::Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeStruct for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T: ?Sized + c_serde::Serialize>(&mut self, _: &'static str, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeTupleStruct for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T: ?Sized + c_serde::Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeStructVariant for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T: c_serde::Serialize + ?Sized>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeMap for &'a mut IVecSer {
    type Ok = ();

    type Error = Error;

    fn serialize_key<T: c_serde::Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(&mut **self)
    }

    fn serialize_value<T: c_serde::Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct IVecDe<'de> {
    data: &'de [u8],
}

pub fn from_ivec<'de, T: Deserialize<'de>>(data: &'de [u8]) -> SResult<T> {
    let mut de = IVecDe {
        data,
    };
    T::deserialize(&mut de)
}

impl<'de> IVecDe<'de> {
    fn take<T: Serialize>(&mut self) -> SResult<T> {
        T::deserialize(&mut self.data)
    }

    fn take_u8(&mut self) -> SResult<u8> {
        Serialize::deserialize(&mut self.data)
    }

    fn take_u16(&mut self) -> SResult<u16> {
        u16::try_from(self.take_u32()?)
            .map_err(|_| Error::Unsupported("attempt to deserialize u16 from other type".to_string()))
    }

    fn take_u32(&mut self) -> SResult<u32> {
        Serialize::deserialize(&mut self.data)
    }

    fn take_u64(&mut self) -> SResult<u64> {
        Serialize::deserialize(&mut self.data)
    }

    fn take_bytes(&mut self) -> SResult<&'de [u8]> {
        let len = self.take_u32()? as usize;
        if self.data.len() < len {
            dbg!(&self.data.len(), len);
            return Err(Error::Unsupported("IVec can't be deserialized".to_string()))
        }
        let res = &self.data[..len];
        self.data = &self.data[len..];
        Ok(res)
    }
}

impl<'de, 'a> Deserializer<'de> for &'a mut IVecDe<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported("attempt to deserialize `Any` type".to_string()))
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_bool(self.take()?)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i8(i8::from_ne_bytes(self.take_u8()?.to_ne_bytes()))
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i16(i16::from_ne_bytes(self.take_u16()?.to_ne_bytes()))
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i32(i32::from_ne_bytes(self.take_u32()?.to_ne_bytes()))
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_i64(i64::from_ne_bytes(self.take_u64()?.to_ne_bytes()))
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u8(self.take_u8()?)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u16(self.take_u16()?)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u32(self.take_u32()?)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u64(self.take_u64()?)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let v = self.take_u32()?;
        visitor.visit_f32(f32::from_ne_bytes(v.to_ne_bytes()))
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let v = self.take_u64()?;
        visitor.visit_f64(f64::from_ne_bytes(v.to_ne_bytes()))
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match char::try_from(self.take_u32()?) {
            Ok(c) => visitor.visit_char(c),
            Err(_) => Err(Error::Unsupported("expected a valid char".to_string()))
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match std::str::from_utf8(self.take_bytes()?) {
            Ok(s) => visitor.visit_borrowed_str(s),
            Err(_) => Err(Error::Unsupported("Cannot deserialize string from invalid utf-8".to_string())),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_bytes(self.take_bytes()?)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if self.take()? {
            visitor.visit_some(self)
        } else {
            visitor.visit_none()
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(self, _name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let seq_len = self.take_u32()? as usize;
        self.deserialize_tuple(seq_len, visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_seq(SeqVisitor{
            de: self,
            len,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(self, _name: &'static str, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let len = self.take_u32()? as usize;
        visitor.visit_map(MapVisitor{
            de: self,
            len,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(self, _name: &'static str, _fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(self, _name: &'static str, _variants: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_enum(EnumVisitor(self))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_u32(self.take_u32()?)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Self::Error> {
        Err(Error::Unsupported("can't deserialize from `Any`".to_string()))
    }
}


struct EnumVisitor<'a, 'de>(&'a mut IVecDe<'de>);
impl<'a, 'de> EnumAccess<'de> for EnumVisitor<'a, 'de> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error> {
        let v = seed.deserialize(&mut *self.0)?;
        Ok((v, self))
    }
}

impl<'a, 'de> VariantAccess<'de> for EnumVisitor<'a, 'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Self::Error> {
        seed.deserialize(self.0)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.0.deserialize_tuple(len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.0.deserialize_struct("", fields, visitor)
    }
}

struct SeqVisitor<'a, 'de> {
    de: &'a mut IVecDe<'de>,
    len: usize,
}

impl<'a, 'de> SeqAccess<'de> for SeqVisitor<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error> {
        if self.len == 0 {
            return Ok(None);
        }
        self.len -= 1;
        Ok(Some(seed.deserialize(&mut *self.de)?))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

struct MapVisitor<'a, 'de> {
    de: &'a mut IVecDe<'de>,
    len: usize,
}

impl<'a, 'de> MapAccess<'de> for MapVisitor<'a, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error> {
        if self.len == 0 {
            return Ok(None);
        }
        self.len -= 1;
        Ok(Some(seed.deserialize(&mut *self.de)?))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Self::Error> {
        seed.deserialize(&mut *self.de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

#[cfg(test)]
mod qc {
    fn serde_type<T>(obj: &T) 
    where
        for<'de> T: c_serde::Serialize + c_serde::Deserialize<'de> + std::fmt::Debug + PartialEq
    {
        let ser = super::to_vec(obj);
        let ser = ser.as_ref();
        let de: T = super::from_ivec(ser).expect("failed to deserialize obj");

        assert_eq!(obj, &de, "serialized and deserialized objects don't match");
    }

    #[test]
    fn serde_primitives() {
        serde_type(&true);
        serde_type(&2_u8);
        serde_type(&-2_i8);
        serde_type(&3_u16);
        serde_type(&-3_i16);
        serde_type(&4_u32);
        serde_type(&-4_i32);
        serde_type(&0xffffffffffffffff_u64);
        serde_type(&-5_i64);
        serde_type(&0xffffffffffffffff_usize);
        serde_type(&-6_isize);
        serde_type(&'\u{20ac}');
        serde_type(&3.2_f32);
        serde_type(&3.2_f64);
        serde_type(&Some(()));
        serde_type(&String::from("Hello!"));
        serde_type(&vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let x: Result<u32, u32> = Ok(1);
        serde_type(&x);

        let mut mymap = std::collections::HashMap::new();
        mymap.insert("abc".to_string(), 123);
        mymap.insert("def".to_string(), 456);

        serde_type(&mymap);
    }
}
