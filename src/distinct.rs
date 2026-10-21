// This file includes source code from https://github.com/jedisct1/rust-hyperloglog/blob/36d73a2c0a324f4122d32febdb19dd4a815147f0/src/hyperloglog/lib.rs under the following BSD 2-Clause "Simplified" License:
//
// Copyright (c) 2013-2025, Frank Denis and Jianshu Zhao
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//   Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
//   Redistributions in binary form must reproduce the above copyright notice, this
//   list of conditions and the following disclaimer in the documentation and/or
//   other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// This file includes source code from https://github.com/codahale/sketchy/blob/09e9ede8ac27e6fd37d5c5f53ac9b7776c37bc19/src/hyperloglog.rs under the following Apache License 2.0:
//
// Copyright (c) 2015-2025 Coda Hale
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// https://github.com/twitter/algebird/blob/5fdb079447271a5fe0f1fba068e5f86591ccde36/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala
// https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD countApproxDistinct
// is_x86_feature_detected ?

// This file includes source code from https://github.com/jedisct1/rust-hyperloglog/... (BSD-2)
// and https://github.com/codahale/sketchy/... (Apache-2.0). See original headers above.

// This file includes source code from https://github.com/jedisct1/rust-hyperloglog/blob/36d73a2c0a324f4122d32febdb19dd4a815147f0/src/hyperloglog/lib.rs under the BSD 2-Clause "Simplified" License.
// This file includes source code from https://github.com/codahale/sketchy/blob/09e9ede8ac27e6fd37d5c5f53ac9b7776c37bc19/src/hyperloglog.rs under the Apache License 2.0.

// https://github.com/twitter/algebird/blob/5fdb079447271a5fe0f1fba068e5f86591ccde36/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala
// https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD countApproxDistinct

// This file includes source code from https://github.com/jedisct1/rust-hyperloglog/blob/36d73a2c0a324f4122d32febdb19dd4a815147f0/src/hyperloglog/lib.rs under the BSD 2-Clause "Simplified" License.
// This file includes source code from https://github.com/codahale/sketchy/blob/09e9ede8ac27e6fd37d5c5f53ac9b7776c37bc19/src/hyperloglog.rs under the Apache License 2.0.

// https://github.com/twitter/algebird/blob/5fdb079447271a5fe0f1fba068e5f86591ccde36/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala
// https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD countApproxDistinct

use serde::{Deserialize, Serialize};
use std::{
	cmp::{self, Ordering}, convert::{identity, TryFrom}, fmt, hash::{Hash, Hasher}, marker::PhantomData, ops::{self, Range}
};
use xxhash_rust::xxh3::Xxh3;

use super::{f64_to_u8, u64_to_f64, usize_to_f64};
use crate::traits::{Intersect, IntersectPlusUnionIsPlus, New, UnionAssign};

mod consts;
use self::consts::{BIAS_DATA, RAW_ESTIMATE_DATA, TRESHOLD_DATA};

/// Like [`HyperLogLog`] but implements `Ord`/`Eq` by comparing estimated cardinalities.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLogMagnitude<V>(HyperLogLog<V>);
impl<V: Hash> Ord for HyperLogLogMagnitude<V> {
	#[inline(always)]
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.len().partial_cmp(&other.0.len()).unwrap()
	}
}
impl<V: Hash> PartialOrd for HyperLogLogMagnitude<V> {
	#[inline(always)]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.0.len().partial_cmp(&other.0.len())
	}
}
impl<V: Hash> PartialEq for HyperLogLogMagnitude<V> {
	#[inline(always)]
	fn eq(&self, other: &Self) -> bool {
		self.0.len().eq(&other.0.len())
	}
}
impl<V: Hash> Eq for HyperLogLogMagnitude<V> {}
impl<V: Hash> Clone for HyperLogLogMagnitude<V> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}
impl<V: Hash> New for HyperLogLogMagnitude<V> {
	type Config = f64;
	fn new(config: &Self::Config) -> Self {
		Self(New::new(config))
	}
}
impl<V: Hash> Intersect for HyperLogLogMagnitude<V> {
	fn intersect<'a>(iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		Intersect::intersect(iter.map(|x| &x.0)).map(Self)
	}
}
impl<'a, V: Hash> UnionAssign<&'a HyperLogLogMagnitude<V>> for HyperLogLogMagnitude<V> {
	fn union_assign(&mut self, rhs: &'a Self) {
		self.0.union_assign(&rhs.0)
	}
}
impl<'a, V: Hash> ops::AddAssign<&'a V> for HyperLogLogMagnitude<V> {
	fn add_assign(&mut self, rhs: &'a V) {
		self.0.add_assign(rhs)
	}
}
impl<'a, V: Hash> ops::AddAssign<&'a Self> for HyperLogLogMagnitude<V> {
	fn add_assign(&mut self, rhs: &'a Self) {
		self.0.add_assign(&rhs.0)
	}
}
impl<V: Hash> fmt::Debug for HyperLogLogMagnitude<V> {
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		self.0.fmt(fmt)
	}
}
impl<V> IntersectPlusUnionIsPlus for HyperLogLogMagnitude<V> {
	const VAL: bool = <HyperLogLog<V> as IntersectPlusUnionIsPlus>::VAL;
}

/// HyperLogLog with bias correction.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLog<V: ?Sized> {
	alpha: f64,
	zero: usize,
	sum: f64,
	p: u8,
	m: Box<[u8]>,
	marker: PhantomData<fn(V)>,
}

impl<V: ?Sized> HyperLogLog<V>
where
	V: Hash,
{
	/// Create an empty HLL from an error tolerance (approx relative error).
	pub fn new(error_rate: f64) -> Self {
		assert!(0.0 < error_rate && error_rate < 1.0);
		let p = f64_to_u8((f64::log2(1.04 / error_rate) * 2.0).ceil());
		// Clamp/validate against supported tables:
		assert!(
			(4..=16).contains(&p),
			"computed p={} from error_rate is outside supported range [4,16]",
			p
		);
		Self::with_p(p)
	}

	/// Create an empty HLL with exactly 2^p registers.
	pub fn with_p(p: u8) -> Self {
		// Keep in sync with your bias/threshold tables
		assert!(
			(4..=16).contains(&p),
			"p out of supported range for bias/threshold tables (have 4..=16)"
		);
		let m = 1usize << p;
		let alpha = Self::get_alpha(p);
		Self {
			alpha,
			zero: m,
			sum: m as f64,
			p,
			m: vec![0u8; m].into_boxed_slice(),
			marker: PhantomData,
		}
	}

	/// Create a new empty HLL using the same settings as `hll`.
	pub fn new_from(hll: &Self) -> Self {
		Self {
			alpha: hll.alpha,
			zero: hll.m.len(),
			sum: usize_to_f64(hll.m.len()),
			p: hll.p,
			m: vec![0; hll.m.len()].into_boxed_slice(),
			marker: PhantomData,
		}
	}

	/// Visit one element.
	#[inline]
	pub fn push(&mut self, value: &V) {
		// xxhash3 (64-bit) hasher
		let mut hasher = Xxh3::default();
		value.hash(&mut hasher);
		let x = hasher.finish();

		let j = x & (self.m.len() as u64 - 1);
		let w = x >> self.p;
		let rho = Self::get_rho(w, 64 - self.p);
		let mjr = &mut self.m[usize::try_from(j).unwrap()];
		let old = *mjr;
		let new = cmp::max(old, rho);
		self.zero -= if old == 0 { 1 } else { 0 };

		// accumulate 2^-register via bit trick.
		self.sum -= f64::from_bits(u64::MAX.wrapping_sub(u64::from(old)) << 54 >> 2)
			- f64::from_bits(u64::MAX.wrapping_sub(u64::from(new)) << 54 >> 2);

		*mjr = new;
	}
	/// Update HLL with an already-uniform 64-bit hash
    pub fn push_hash64(&mut self, x: u64) {
        let j = (x & ((self.m.len() as u64) - 1)) as usize;
        let w = x >> self.p;
        let rho = Self::get_rho(w, 64 - self.p);

        let mjr = &mut self.m[j];
        let old = *mjr;
        let new = old.max(rho);

        if old == 0 {
            self.zero -= 1;
        }

        // subtract 2^-old and add 2^-new (same bithack used in `push`)
        self.sum -= f64::from_bits(u64::MAX.wrapping_sub(old as u64) << 54 >> 2)
                 - f64::from_bits(u64::MAX.wrapping_sub(new as u64) << 54 >> 2);

        *mjr = new;
    }

	/// Estimated cardinality.
	pub fn len(&self) -> f64 {
		let v = self.zero;
		if v > 0 {
			let h =
				usize_to_f64(self.m.len()) * (usize_to_f64(self.m.len()) / usize_to_f64(v)).ln();
			if h <= Self::get_threshold(self.p - 4) {
				return h;
			}
		}
		self.ep()
	}

	/// Is empty?
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.zero == self.m.len()
	}

	/// Merge (union) another HLL into `self`.
	pub fn union(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());

		// SIMD path (portable)
		#[cfg(feature = "stdsimd")]
		{
			let (zero, sum) = simd_union_assign(&mut self.m, &src.m);
			self.zero = zero;
			self.sum = sum;
			return;
		}

		// Scalar fallback
		#[cfg(not(feature = "stdsimd"))]
		{
			let mut zero = 0usize;
			let mut sum = 0.0f64;
			for (to, from) in self.m.iter_mut().zip(src.m.iter()) {
				*to = (*to).max(*from);
				zero += if *to == 0 { 1 } else { 0 };
				sum += f64::from_bits(u64::MAX.wrapping_sub(u64::from(*to)) << 54 >> 2);
			}
			self.zero = zero;
			self.sum = sum;
		}
	}

	/// Intersect registers (lane-wise min). **Note:** not a set-intersection estimator.
	pub fn intersect(&mut self, src: &Self) {
		assert_eq!(src.alpha, self.alpha);
		assert_eq!(src.p, self.p);
		assert_eq!(src.m.len(), self.m.len());

		// SIMD path (portable)
		#[cfg(feature = "stdsimd")]
		{
			let (zero, sum) = simd_intersect_assign(&mut self.m, &src.m);
			self.zero = zero;
			self.sum = sum;
			return;
		}

		// Scalar fallback
		#[cfg(not(feature = "stdsimd"))]
		{
			let mut zero = 0usize;
			let mut sum = 0.0f64;
			for (to, from) in self.m.iter_mut().zip(src.m.iter()) {
				*to = (*to).min(*from);
				zero += if *to == 0 { 1 } else { 0 };
				sum += f64::from_bits(u64::MAX.wrapping_sub(u64::from(*to)) << 54 >> 2);
			}
			self.zero = zero;
			self.sum = sum;
		}
	}

	/// Reset to the empty state.
	pub fn clear(&mut self) {
		self.zero = self.m.len();
		self.sum = usize_to_f64(self.m.len());
		self.m.iter_mut().for_each(|x| *x = 0);
	}

	#[inline]
	fn get_threshold(p: u8) -> f64 {
		TRESHOLD_DATA[p as usize]
	}

	fn get_alpha(p: u8) -> f64 {
		assert!(4 <= p && p <= 16);
		match p {
			4 => 0.673,
			5 => 0.697,
			6 => 0.709,
			_ => 0.7213 / (1.0 + 1.079 / u64_to_f64(1_u64 << p)),
		}
	}

	#[inline]
	fn get_rho(w: u64, max_width: u8) -> u8 {
		let rho = max_width - (64 - u8::try_from(w.leading_zeros()).unwrap()) + 1;
		assert!(0 < rho && rho < 65);
		rho
	}

	fn estimate_bias(e: f64, p: u8) -> f64 {
		let bias_vector = BIAS_DATA[(p - 4) as usize];
		let neighbors = Self::get_nearest_neighbors(e, RAW_ESTIMATE_DATA[(p - 4) as usize]);
		assert_eq!(neighbors.len(), 6);
		bias_vector[neighbors].iter().sum::<f64>() / 6.0_f64
	}

	#[cfg(feature = "serde")]
	/// Serialize this HLL to a writer using bincode.
	pub fn save<W: std::io::Write>(&self, mut writer: W) -> std::io::Result<()> {
		bincode::serialize_into(&mut writer, &self)
			.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
	}

	#[cfg(feature = "serde")]
	/// Load an HLL from a reader using bincode, with basic validation.
	pub fn load<R: std::io::Read>(mut reader: R) -> std::io::Result<Self> {
		use std::io;
		use bincode::ErrorKind as BinErr;

		// 1) Deserialize and propagate IO kinds (including UnexpectedEof)
		let mut sketch: Self = match bincode::deserialize_from::<_, Self>(&mut reader) {
			Ok(s) => s,
			Err(e) => {
				match *e {
					BinErr::Io(ref ioe) => {
						// Recreate an io::Error with the same kind & message.
						return Err(io::Error::new(ioe.kind(), ioe.to_string()));
					}
					_ => {
						// Non-IO bincode error.
						return Err(io::Error::new(io::ErrorKind::Other, e));
					}
				}
			}
		};

		// 2) Validate p and register length
		if !(4..=16).contains(&sketch.p) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("invalid p={} (expected 4..=16)", sketch.p),
			));
		}
		let expected_m = 1usize << sketch.p;
		if sketch.m.len() != expected_m {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("state length {} does not match 2^p ({})", sketch.m.len(), expected_m),
			));
		}

		// 3) Recompute derived fields from the registers
		sketch.alpha = Self::get_alpha(sketch.p);
		sketch.zero = sketch.m.iter().filter(|&&r| r == 0).count();
		sketch.sum = sketch
			.m
			.iter()
			.map(|&r| f64::from_bits(u64::MAX.wrapping_sub(r as u64) << 54 >> 2))
			.sum::<f64>();

		Ok(sketch)
	}

	fn get_nearest_neighbors(e: f64, estimate_vector: &[f64]) -> Range<usize> {
		let index = estimate_vector
			.binary_search_by(|a| a.partial_cmp(&e).unwrap_or(Ordering::Equal))
			.unwrap_or_else(identity);

		let mut min = if index > 6 { index - 6 } else { 0 };
		let mut max = cmp::min(index + 6, estimate_vector.len());

		while max - min != 6 {
			let (min_val, max_val) = unsafe {
				(
					*estimate_vector.get_unchecked(min),
					*estimate_vector.get_unchecked(max - 1),
				)
			};
			if 2.0 * e - min_val > max_val {
				min += 1;
			} else {
				max -= 1;
			}
		}

		min..max
	}

	#[inline]
	fn ep(&self) -> f64 {
		let e = self.alpha * usize_to_f64(self.m.len() * self.m.len()) / self.sum;
		if e <= usize_to_f64(5 * self.m.len()) {
			e - Self::estimate_bias(e, self.p)
		} else {
			e
		}
	}
}

impl<V: ?Sized> Clone for HyperLogLog<V> {
	fn clone(&self) -> Self {
		Self {
			alpha: self.alpha,
			zero: self.zero,
			sum: self.sum,
			p: self.p,
			m: self.m.clone(),
			marker: PhantomData,
		}
	}
}
impl<V: ?Sized> fmt::Debug for HyperLogLog<V>
where
	V: Hash,
{
	fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
		fmt.debug_struct("HyperLogLog")
			.field("len", &self.len())
			.finish()
	}
}
impl<V: ?Sized> New for HyperLogLog<V>
where
	V: Hash,
{
	type Config = f64;
	fn new(config: &Self::Config) -> Self {
		Self::new(*config)
	}
}
impl<V: ?Sized> Intersect for HyperLogLog<V>
where
	V: Hash,
{
	fn intersect<'a>(mut iter: impl Iterator<Item = &'a Self>) -> Option<Self>
	where
		Self: Sized + 'a,
	{
		let mut ret = iter.next()?.clone();
		iter.for_each(|x| ret.intersect(x));
		Some(ret)
	}
}
impl<'a, V: ?Sized> UnionAssign<&'a HyperLogLog<V>> for HyperLogLog<V>
where
	V: Hash,
{
	fn union_assign(&mut self, rhs: &'a Self) {
		self.union(rhs)
	}
}
impl<'a, V: ?Sized> ops::AddAssign<&'a V> for HyperLogLog<V>
where
	V: Hash,
{
	fn add_assign(&mut self, rhs: &'a V) {
		self.push(rhs)
	}
}
impl<'a, V: ?Sized> ops::AddAssign<&'a Self> for HyperLogLog<V>
where
	V: Hash,
{
	fn add_assign(&mut self, rhs: &'a Self) {
		self.union(rhs)
	}
}
impl<V: ?Sized> IntersectPlusUnionIsPlus for HyperLogLog<V> {
	const VAL: bool = true;
}

#[cfg(feature = "stdsimd")]
mod simd_portable {
	use std::simd::{
		num::SimdUint, prelude::{SimdFloat, SimdOrd, SimdPartialEq}, Simd
	};

	const LANES: usize = 64;

	#[inline]
	fn powbits_sum_f32(v: Simd<u8, LANES>) -> f64 {
		// ((u32::MAX - x) << 25) >> 2  -> reinterpret as f32, then sum
		let x_u32: Simd<u32, LANES> = v.cast();
		let t = ((Simd::splat(u32::MAX) - x_u32) << Simd::splat(25)) >> Simd::splat(2);
		let f: Simd<f32, LANES> = Simd::from_bits(t);
		f.reduce_sum() as f64
	}

	#[inline]
	fn zero_lanes(v: Simd<u8, LANES>) -> usize {
		let m = v.simd_eq(Simd::splat(0));
		m.to_bitmask().count_ones() as usize
	}

	/// Lane-wise max (union) and recompute zero/sum.
	pub fn simd_union_assign(a: &mut [u8], b: &[u8]) -> (usize, f64) {
		let (a_chunks, a_rem) = a.as_chunks_mut::<LANES>();
		let (b_chunks, b_rem) = b.as_chunks::<LANES>();
		let mut zero = 0usize;
		let mut sum = 0.0f64;

		for (aa, bb) in a_chunks.iter_mut().zip(b_chunks.iter()) {
			let av = Simd::<u8, LANES>::from_array(*aa);
			let bv = Simd::<u8, LANES>::from_array(*bb);
			let rv = av.simd_max(bv);
			zero += zero_lanes(rv);
			sum += powbits_sum_f32(rv);
			*aa = rv.to_array();
		}

		for (to, from) in a_rem.iter_mut().zip(b_rem.iter()) {
			let v = (*to).max(*from);
			*to = v;
			if v == 0 {
				zero += 1;
			}
			sum += f64::from_bits(u64::MAX.wrapping_sub(u64::from(v)) << 54 >> 2);
		}

		(zero, sum)
	}

	/// Lane-wise min (intersect) and recompute zero/sum.
	pub fn simd_intersect_assign(a: &mut [u8], b: &[u8]) -> (usize, f64) {
		let (a_chunks, a_rem) = a.as_chunks_mut::<LANES>();
		let (b_chunks, b_rem) = b.as_chunks::<LANES>();
		let mut zero = 0usize;
		let mut sum = 0.0f64;

		for (aa, bb) in a_chunks.iter_mut().zip(b_chunks.iter()) {
			let av = Simd::<u8, LANES>::from_array(*aa);
			let bv = Simd::<u8, LANES>::from_array(*bb);
			let rv = av.simd_min(bv);
			zero += zero_lanes(rv);
			sum += powbits_sum_f32(rv);
			*aa = rv.to_array();
		}

		for (to, from) in a_rem.iter_mut().zip(b_rem.iter()) {
			let v = (*to).min(*from);
			*to = v;
			if v == 0 {
				zero += 1;
			}
			sum += f64::from_bits(u64::MAX.wrapping_sub(u64::from(v)) << 54 >> 2);
		}

		(zero, sum)
	}
}

// Bring the SIMD functions into the parent module’s scope when enabled:
#[cfg(feature = "stdsimd")]
use simd_portable::{simd_intersect_assign, simd_union_assign};

#[cfg(test)]
mod test {
	use super::{super::f64_to_usize, HyperLogLog};
	use std::f64;
	use std::io::Write;
	#[test]
	fn pow_bithack() {
		for x in 0_u8..65 {
			let a = 2.0_f64.powi(-(i32::from(x)));
			let b = f64::from_bits(u64::MAX.wrapping_sub(u64::from(x)) << 54 >> 2);
			let c = f32::from_bits(u32::MAX.wrapping_sub(u32::from(x)) << 25 >> 2);
			assert_eq!(a, b);
			assert_eq!(a, f64::from(c));
		}
	}

	#[test]
	fn hyperloglog_test_simple() {
		let mut hll = HyperLogLog::new(0.00408);
		let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
		for k in &keys {
			hll.push(k);
		}
		assert!((hll.len().round() - 3.0).abs() < f64::EPSILON);
		assert!(!hll.is_empty());
		hll.clear();
		assert!(hll.is_empty());
		assert!(hll.len() == 0.0);
	}

	#[test]
	fn hyperloglog_test_merge() {
		let mut hll = HyperLogLog::new(0.00408);
		let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
		for k in &keys {
			hll.push(k);
		}
		assert!((hll.len().round() - 3.0).abs() < f64::EPSILON);

		let mut hll2 = HyperLogLog::new_from(&hll);
		let keys2 = ["test3", "test4", "test4", "test4", "test4", "test1"];
		for k in &keys2 {
			hll2.push(k);
		}
		assert!((hll2.len().round() - 3.0).abs() < f64::EPSILON);

		hll.union(&hll2);
		assert!((hll.len().round() - 4.0).abs() < f64::EPSILON);
	}

	#[test]
	fn push() {
		let actual = 100_000.0;
		let p = 0.05;
		let mut hll = HyperLogLog::new(p);
		for i in 0..f64_to_usize(actual) {
			hll.push(&i);
		}
		assert!(hll.len() > (actual - (actual * p * 3.0)));
		assert!(hll.len() < (actual + (actual * p * 3.0)));
	}
	#[test]
	fn push_hash64() {
		use std::hash::Hash;
		use xxhash_rust::xxh3::Xxh3;
		use std::hash::Hasher;

		// Same precision for both sketches
		let mut h_from_val = HyperLogLog::new(0.01);
		let mut h_from_hash = HyperLogLog::new_from(&h_from_val);

		// Feed 10k distinct items both ways:
		//  - h_from_val: via `push(&value)` (internally hashes with Xxh3)
		//  - h_from_hash: via `push_hash64(x)` where x is the same Xxh3 hash
		for i in 0u64..10_000 {
			let mut hasher = Xxh3::default();
			i.hash(&mut hasher);
			let x = hasher.finish();

			h_from_val.push(&i);
			h_from_hash.push_hash64(x);
		}

		let a = h_from_val.len();
		let b = h_from_hash.len();

		// They should be identical (same registers updated in the same order).
		assert!((a - b).abs() < f64::EPSILON, "len mismatch: a={a}, b={b}");
		// Quick sanity: ballpark distinct count is close to 10k
		assert!(a > 9000.0 && a < 11000.0, "unexpected estimate: {a}");
	}
	#[cfg(feature = "serde")]
	#[test]
	fn hll_save_and_load_roundtrip() {
		use std::fs::{remove_file, File};
		use std::io::{BufReader, BufWriter};

		// Build a sketch and feed it some items
		let mut hll = HyperLogLog::with_p(12); // 4096 registers, ~1.6% RSE
		for i in 0u64..10_000 {
			hll.push(&i);
		}
		let est_before = hll.len();

		// Save to disk
		let path = "test_hll.bin";
		{
			let f = File::create(path).expect("create file");
			let w = BufWriter::new(f);
			hll.save(w).expect("save hll");
		}

		// Load back
		let loaded = {
			let f = File::open(path).expect("open file");
			let r = BufReader::new(f);
			HyperLogLog::<u8>::load(r).expect("load hll") // the V type is phantom; any V is fine
		};

		// Registers should match exactly
		assert_eq!(loaded.p, hll.p);
		assert_eq!(loaded.m.len(), hll.m.len());
		assert_eq!(&*loaded.m, &*hll.m, "register arrays differ after load");

		// Estimate should be (very) close; allow a tiny FP drift
		let est_after = loaded.len();
		let diff = (est_after - est_before).abs();
		assert!(
			diff <= 1e-12,
			"estimate drift too large: before={est_before}, after={est_after}, diff={diff}"
		);

		remove_file(path).ok();
	}
	#[cfg(feature = "serde")]
	#[test]
	fn hll_save_two_then_load_back_streaming_cardinality() -> std::io::Result<()> {
		use super::HyperLogLog;
		use std::fs::{remove_file, File};
		use std::io::{BufReader, BufWriter};

		// Two sketches with 2^12 registers
		let mut s1 = HyperLogLog::<u64>::with_p(12);
		let mut s2 = HyperLogLog::<u64>::with_p(12);

		// s1: 0..10_000 (10k uniques)
		for i in 0u64..10_000 {
			s1.push(&i);
		}
		// s2: 5_000..15_000 (10k uniques), 5k overlap with s1
		for i in 5_000u64..15_000 {
			s2.push(&i);
		}

		// Ground-truth cardinalities
		let t1 = 10_000.0;
		let t2 = 10_000.0;
		let tu = 15_000.0;
		let rel_tol = 0.05; // 5% tolerance (>> ~1.6% RSE for p=12)

		// Check before saving
		let e1 = s1.len();
		let e2 = s2.len();
		let mut u = s1.clone();
		u.union(&s2);
		let eu = u.len();

		assert!((e1 - t1).abs() <= t1 * rel_tol, "s1 est={} truth={}", e1, t1);
		assert!((e2 - t2).abs() <= t2 * rel_tol, "s2 est={} truth={}", e2, t2);
		assert!((eu - tu).abs() <= tu * rel_tol, "union est={} truth={}", eu, tu);

		// Save both sketches back-to-back
		let path = "test_hll_stream_two_card.bin";
		{
			let f = File::create(path)?;
			let mut w = BufWriter::new(f);
			s1.save(&mut w)?;
			s2.save(&mut w)?;
			w.flush()?;
		}

		// Load both back
		let (s1b, s2b) = {
			let f = File::open(path)?;
			let mut r = BufReader::new(f);
			let a = HyperLogLog::<u64>::load(&mut r)?;
			let b = HyperLogLog::<u64>::load(&mut r)?;
			(a, b)
		};

		// Check after loading (still close to truth)...
		let e1b = s1b.len();
		let e2b = s2b.len();
		let mut ub = s1b.clone();
		ub.union(&s2b);
		let eub = ub.len();

		assert!((e1b - t1).abs() <= t1 * rel_tol, "s1(load) est={} truth={}", e1b, t1);
		assert!((e2b - t2).abs() <= t2 * rel_tol, "s2(load) est={} truth={}", e2b, t2);
		assert!((eub - tu).abs() <= tu * rel_tol, "union(load) est={} truth={}", eub, tu);

		// ...and unchanged by serialization
		assert!((e1 - e1b).abs() < 1e-9, "s1 est changed by save/load");
		assert!((e2 - e2b).abs() < 1e-9, "s2 est changed by save/load");
		assert!((eu - eub).abs() < 1e-9, "union est changed by save/load");

		remove_file(path).ok();
		Ok(())
	}

	fn assert_hll_register_merges(left: &HyperLogLog<u64>, right: &HyperLogLog<u64>) {
		for intersect in [false, true] {
			let expected: Vec<u8> = left
				.m
				.iter()
				.zip(right.m.iter())
				.map(|(&a, &b)| if intersect { a.min(b) } else { a.max(b) })
				.collect();
			let expected_zero = expected.iter().filter(|&&rank| rank == 0).count();
			let expected_sum: f64 = expected
				.iter()
				.map(|&rank| 2.0_f64.powi(-i32::from(rank)))
				.sum();
			let mut actual = left.clone();
			if intersect {
				actual.intersect(right);
			} else {
				actual.union(right);
			}
			assert_eq!(
				&*actual.m, &*expected,
				"p={}, intersect={intersect}",
				left.p
			);
			assert_eq!(
				actual.zero, expected_zero,
				"p={}, intersect={intersect}",
				left.p
			);
			// SIMD reduces each chunk in f32 before accumulating into f64.
			assert!(
				(actual.sum - expected_sum).abs() <= expected_sum * 1e-6,
				"p={}, intersect={intersect}: sum={} expected={expected_sum}",
				left.p,
				actual.sum
			);
		}
	}

	#[test]
	fn hll_merge_empty_registers() {
		for p in [4, 5, 6, 10, 16] {
			let empty = HyperLogLog::<u64>::with_p(p);
			assert_hll_register_merges(&empty, &empty);
		}
	}

	#[test]
	fn hll_merge_mixed_registers() {
		// With 64 lanes, p=4/5 uses only the scalar tail, p>=6 only full chunks.
		for p in [4, 5, 6, 10, 16] {
			let empty = HyperLogLog::<u64>::with_p(p);
			let mut left = empty.clone();
			let mut right = empty.clone();
			let ranks = [0, 1, 2, 3, 24, 32, 48, 65 - p];
			for (phase, sketch) in [(0, &mut left), (3, &mut right)] {
				for (i, rank) in sketch.m.iter_mut().enumerate() {
					*rank = if i % 11 == 0 {
						0
					} else {
						ranks[(i * 5 + phase) % ranks.len()]
					};
				}
				sketch.zero = sketch.m.iter().filter(|&&rank| rank == 0).count();
				sketch.sum = sketch
					.m
					.iter()
					.map(|&rank| 2.0_f64.powi(-i32::from(rank)))
					.sum();
			}
			assert_hll_register_merges(&left, &right);
			assert_hll_register_merges(&right, &left);
			assert_hll_register_merges(&left, &left);
			assert_hll_register_merges(&left, &empty);
			assert_hll_register_merges(&empty, &right);
		}
	}

	#[test]
	fn hll_merge_hashed_registers() {
		for p in [4, 5, 6, 10, 16] {
			let mut left = HyperLogLog::<u64>::with_p(p);
			let mut right = HyperLogLog::<u64>::with_p(p);
			for value in 0..2048_u64 {
				left.push(&value);
				right.push(&(value + 1024));
			}
			assert_hll_register_merges(&left, &right);
		}
	}
}
