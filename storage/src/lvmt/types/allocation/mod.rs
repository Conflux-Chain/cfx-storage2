mod allocate_position;
mod key_info;

pub use allocate_position::AllocatePosition;
pub use key_info::AllocationKeyInfo;

pub const SLOT_SIZE: usize = 6;
pub const KEY_SLOT_SIZE: usize = SLOT_SIZE - 1;
