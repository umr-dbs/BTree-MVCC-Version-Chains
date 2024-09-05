pub trait UnsafeClone {
    unsafe fn unsafe_clone(&self) -> Self;
}