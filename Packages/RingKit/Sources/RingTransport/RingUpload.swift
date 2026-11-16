import RingProtocol

/// A frame the ring sent on its own rather than in reply to a request, as delivered by
/// `RingSession.uploads()`.
///
/// The link does not say which characteristic a frame arrived on, so the kind is decided by the
/// frame's group. Which characteristic carries live values on the R99 has not been checked.
public enum RingUpload: Equatable, Sendable {
    /// A ring-initiated event (group `0x04`), such as `0x040E` when an on-demand measurement
    /// finishes. The session has already queued its acknowledgement; subscribers must not send one.
    case event(Frame)
    /// A live value (group `0x06`), such as on-demand heart rate on `0x0601`. For display only:
    /// readings reach history through the ring's stored records, not through these frames.
    case live(Frame)
}
