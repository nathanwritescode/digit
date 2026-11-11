import Foundation

/// Hexadecimal text for byte arrays, used by capture files, `ringprobe` and tests.
public enum Hex {
    /// Lowercase, two digits per byte, no separators.
    public static func encode(_ bytes: [UInt8]) -> String {
        bytes.map { String(format: "%02x", $0) }.joined()
    }

    /// Returns `nil` unless `text` is an even number of hex digits (either case) and nothing else.
    public static func decode(_ text: String) -> [UInt8]? {
        // UInt8(_:radix:) accepts a leading sign, so check the characters first.
        guard text.count % 2 == 0, text.allSatisfy(\.isHexDigit) else { return nil }
        var bytes: [UInt8] = []
        var index = text.startIndex
        while index < text.endIndex {
            let next = text.index(index, offsetBy: 2)
            guard let byte = UInt8(text[index..<next], radix: 16) else { return nil }
            bytes.append(byte)
            index = next
        }
        return bytes
    }
}
