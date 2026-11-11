import Foundation

/// One completed, checksum-verified history transfer, with the context needed to decode it later.
///
/// Ring timestamps are local wall-clock time with no zone, so the raw bytes can only be turned
/// back into correct dates if the zone in effect when they were fetched is kept with them.
/// In JSON the byte fields are hex strings.
public struct HistoryTransfer: Equatable, Sendable, Codable {
    public var kind: HistoryKind
    /// Payload of the header frame the ring sent in answer to the request (9 bytes or fewer when empty).
    public var header: [UInt8]
    /// The data frames' payloads, concatenated. Empty when the ring had nothing stored.
    public var data: [UInt8]
    public var fetchedAt: Date
    /// Identifier of the time zone in effect at `fetchedAt` (for example `Europe/London`).
    public var timeZoneIdentifier: String

    public init(kind: HistoryKind, header: [UInt8], data: [UInt8], fetchedAt: Date, timeZoneIdentifier: String) {
        self.kind = kind
        self.header = header
        self.data = data
        self.fetchedAt = fetchedAt
        self.timeZoneIdentifier = timeZoneIdentifier
    }

    private enum CodingKeys: String, CodingKey {
        case kind, header, data, fetchedAt, timeZoneIdentifier
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        kind = try c.decode(HistoryKind.self, forKey: .kind)
        header = try Self.bytes(c, .header)
        data = try Self.bytes(c, .data)
        fetchedAt = try c.decode(Date.self, forKey: .fetchedAt)
        timeZoneIdentifier = try c.decode(String.self, forKey: .timeZoneIdentifier)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(kind, forKey: .kind)
        try c.encode(Hex.encode(header), forKey: .header)
        try c.encode(Hex.encode(data), forKey: .data)
        try c.encode(fetchedAt, forKey: .fetchedAt)
        try c.encode(timeZoneIdentifier, forKey: .timeZoneIdentifier)
    }

    private static func bytes(_ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys) throws -> [UInt8] {
        let text = try c.decode(String.self, forKey: key)
        guard let bytes = Hex.decode(text) else {
            throw DecodingError.dataCorruptedError(forKey: key, in: c, debugDescription: "not a hex string")
        }
        return bytes
    }
}
