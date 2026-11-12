import Foundation

/// One completed, checksum-verified history transfer, with the context needed to decode it later.
///
/// Ring timestamps are local wall-clock time with no zone, so the raw bytes can only be turned
/// back into correct dates if the zone in effect when they were fetched is kept with them.
/// In JSON the byte fields are hex strings, `kind` is its name and `fetchedAt` is an ISO 8601
/// string in UTC with millisecond precision, whatever the encoder's own date settings.
public struct HistoryTransfer: Equatable, Sendable, Codable {
    public var kind: HistoryKind
    /// Payload of the header frame the ring sent in answer to the request (9 bytes or fewer when empty).
    public var header: [UInt8]
    /// The data frames' payloads, concatenated. Empty when the ring had nothing stored.
    public var data: [UInt8]
    /// Whole milliseconds, the precision the file format keeps.
    public var fetchedAt: Date
    /// Identifier of the time zone in effect on the phone or Mac at `fetchedAt` (for example
    /// `Europe/London`). The ring stamps records with whatever clock was last set on it and does
    /// not follow travel or daylight saving, so this only approximates the zone the records were
    /// made in. Ingestion must not treat it as authoritative.
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
        let name = try c.decode(String.self, forKey: .kind)
        guard let kind = HistoryKind.allCases.first(where: { $0.name == name }) else {
            throw DecodingError.dataCorruptedError(forKey: .kind, in: c, debugDescription: "unknown history kind '\(name)'")
        }
        self.kind = kind
        header = try Self.bytes(c, .header)
        data = try Self.bytes(c, .data)
        let stamp = try c.decode(String.self, forKey: .fetchedAt)
        guard let date = Self.dateFormatter().date(from: stamp) else {
            throw DecodingError.dataCorruptedError(forKey: .fetchedAt, in: c, debugDescription: "not an ISO 8601 date")
        }
        fetchedAt = date
        timeZoneIdentifier = try c.decode(String.self, forKey: .timeZoneIdentifier)
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(kind.name, forKey: .kind)
        try c.encode(Hex.encode(header), forKey: .header)
        try c.encode(Hex.encode(data), forKey: .data)
        try c.encode(Self.dateFormatter().string(from: fetchedAt), forKey: .fetchedAt)
        try c.encode(timeZoneIdentifier, forKey: .timeZoneIdentifier)
    }

    /// Whole milliseconds, which `fetchedAt` is truncated to so that the encoded text decodes to an equal value.
    /// Idempotent: it goes through the same integer conversion the store uses.
    public static func truncatedToMilliseconds(_ date: Date) -> Date {
        self.date(millisecondsSince1970: millisecondsSince1970(date))
    }

    /// The date's whole milliseconds since 1970, rounding toward negative infinity: the largest
    /// `ms` with `date(millisecondsSince1970: ms) <= date`. Defined by comparing real `Date`s
    /// (not just `floor(t * 1000)`, which floating point can leave one off, and `Date` itself
    /// stores an offset from 2001), so converting back and forth always returns the same integer.
    public static func millisecondsSince1970(_ date: Date) -> Int64 {
        var ms = Int64((date.timeIntervalSince1970 * 1000).rounded(.down))
        while self.date(millisecondsSince1970: ms + 1) <= date { ms += 1 }
        while self.date(millisecondsSince1970: ms) > date { ms -= 1 }
        return ms
    }

    public static func date(millisecondsSince1970 ms: Int64) -> Date {
        Date(timeIntervalSince1970: Double(ms) / 1000)
    }

    // A fresh formatter each time: ISO8601DateFormatter is not Sendable.
    private static func dateFormatter() -> ISO8601DateFormatter {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        f.timeZone = TimeZone(identifier: "UTC")
        return f
    }

    private static func bytes(_ c: KeyedDecodingContainer<CodingKeys>, _ key: CodingKeys) throws -> [UInt8] {
        let text = try c.decode(String.self, forKey: key)
        guard let bytes = Hex.decode(text) else {
            throw DecodingError.dataCorruptedError(forKey: key, in: c, debugDescription: "not a hex string")
        }
        return bytes
    }
}
