use uuid::Uuid;

/// Versions are referred to with UUIDs.
pub type VersionId = Uuid;

/// The distinguished value for "no version"
pub const NIL_VERSION_ID: VersionId = Uuid::nil();
