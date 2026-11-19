import DigitCore
import Foundation
import Testing

@Test(.timeLimit(.minutes(1))) func databaseLivesInADedicatedSubdirectoryOfTheContainer() {
    let container = URL(filePath: "/container", directoryHint: .isDirectory)
    let url = DatabaseLocation.databaseURL(inAppGroupContainer: container)
    #expect(url.path == "/container/Digit/Store/digit.sqlite")
    #expect(url.deletingLastPathComponent().path == "/container/Digit/Store")
}

@Test(.timeLimit(.minutes(1))) func openFailureTextHoldsNoPath() {
    struct Synthetic: Error {}
    let text = DatabaseLocation.openFailureMessage(CocoaError(.fileWriteNoPermission, userInfo: [NSFilePathErrorKey: "/private/secret/path"]))
    #expect(!text.contains("/private/secret/path"))
    #expect(!DatabaseLocation.openFailureMessage(Synthetic()).isEmpty)
}
