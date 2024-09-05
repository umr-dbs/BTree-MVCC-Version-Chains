use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::{Range, RangeInclusive};

pub type U64interval = Interval<u64>;
// pub type VersionInterval = Interval<Version>;

#[derive(Eq, PartialEq, Hash, Default, Clone, Serialize, Deserialize)]
pub struct Interval<E: Ord + Copy + Hash> {
    pub lower: E,
    pub upper: E,
}

impl U64interval {
    #[inline(always)]
    pub(crate) const fn blank() -> Self {
        Self {
            lower: u64::MAX,
            upper: u64::MIN,
        }
    }

    #[inline(always)]
    pub(crate) const fn max() -> Self {
        Self {
            lower: u64::MIN,
            upper: u64::MAX,
        }
    }

    #[inline(always)]
    pub(crate) const fn is_upper_max(&self) -> bool {
        self.upper == u64::MAX
    }

    #[inline(always)]
    pub(crate) const fn is_lower_min(&self) -> bool {
        self.lower == u64::MIN
    }
}

impl<E: Ord + Copy + Hash> Interval<E> {
    pub const fn new(lower: E, upper: E) -> Self {
        Self { lower, upper }
    }

    #[inline(always)]
    pub fn set_lower(&mut self, e: E) {
        self.lower = e;
    }

    #[inline(always)]
    pub fn set_upper(&mut self, e: E) {
        self.upper = e;
    }

    #[inline(always)]
    pub const fn lower(&self) -> E {
        self.lower
    }

    #[inline(always)]
    pub const fn upper(&self) -> E {
        self.upper
    }

    #[inline(always)]
    pub fn merge(mut self, interval: &Self) -> Self {
        self.lower = interval.lower.min(self.lower);
        self.upper = interval.upper.max(self.upper);
        self
    }

    #[inline(always)]
    pub fn merge_mut(&mut self, interval: &Self) -> &mut Self {
        self.lower = interval.lower.min(self.lower);
        self.upper = interval.upper.max(self.upper);
        self
    }

    #[inline(always)]
    pub fn merged(&mut self, interval: &Self) {
        self.lower = interval.lower.min(self.lower);
        self.upper = interval.upper.max(self.upper);
    }

    #[inline(always)]
    pub fn expand(mut self, e: E) -> Self {
        self.lower = self.lower.min(e);
        self.upper = self.upper.max(e);
        self
    }

    #[inline(always)]
    pub fn expanded(&mut self, e: E) {
        self.lower = self.lower.min(e);
        self.upper = self.upper.max(e);
    }

    #[inline(always)]
    pub fn expand_mut(&mut self, e: E) -> &mut Self {
        self.lower = self.lower.min(e);
        self.upper = self.upper.max(e);
        self
    }

    #[inline(always)]
    pub fn intersection(&self, other: &Self) -> Self {
        Self::new(
            E::max(self.lower, other.lower),
            E::min(self.upper, other.upper),
        )
    }

    #[inline(always)]
    pub fn covers(&self, other: &Self) -> bool {
        self.lower <= other.lower && self.upper >= other.upper
    }

    #[inline(always)]
    pub fn covers_or_merge(&mut self, other: &Self) -> bool {
        self.covers(other).then(|| true).unwrap_or_else(|| {
            self.merged(other);
            false
        })
    }

    #[inline(always)]
    pub fn overlap(&self, other: &Self) -> bool {
        !self.is_disjoint(other)
    }

    #[inline(always)]
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.lower > other.upper || other.lower > self.upper
    }

    #[inline(always)]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.lower >= other.lower && self.upper <= other.upper
    }

    #[inline(always)]
    pub fn contains(&self, value: E) -> bool {
        value >= self.lower && value <= self.upper
    }
}

impl<E: Ord + Copy + Hash> Into<Interval<E>> for (E, E) {
    fn into(self) -> Interval<E> {
        Interval::new(self.0, self.1)
    }
}

impl<E: Ord + Copy + Hash> Into<Interval<E>> for RangeInclusive<E> {
    fn into(self) -> Interval<E> {
        Interval::new(*self.start(), *self.end())
    }
}

impl Into<U64interval> for Range<u64> {
    fn into(self) -> U64interval {
        U64interval::new(self.start, self.end.checked_add(1).unwrap_or(u64::MAX))
    }
}

impl<E: Ord + Copy + Hash> PartialOrd<Self> for Interval<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.lower.partial_cmp(&other.lower)
    }
}

impl<E: Ord + Copy + Hash> Ord for Interval<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.lower.cmp(&other.lower)
    }
}

impl<E: Ord + Copy + Hash + Display> Display for Interval<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(low: {}, high: {})", self.lower, self.upper)
    }
}
