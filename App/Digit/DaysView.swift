import DigitCore
import SwiftUI

/// Steps, distance and heart rate for each of the last 30 days that has any, newest first. No
/// charts yet.
struct DaysView: View {
    let days: DailyModel

    var body: some View {
        DataStateView(state: days.state, subject: "steps and heart rate", retry: { await days.reload() }) { rows in
            List(rows) { row in
                DayRowView(row: row)
                    .accessibilityElement(children: .ignore)
                    .accessibilityLabel(row.accessibilityLabel)
                    .accessibilityHint(row.uncertaintyExplanation ?? "")
            }
            .refreshable { await days.reload() }
        }
        .navigationTitle("Days")
    }
}

struct DayRowView: View {
    let row: DayRow

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(row.title)
                .font(.headline)
            HStack(alignment: .firstTextBaseline) {
                Label(row.stepsText, systemImage: "figure.walk")
                Spacer()
                Text(row.distanceText)
                    .foregroundStyle(.secondary)
            }
            .monospacedDigit()
            if let heartRate = row.heartRateText {
                Label(heartRate, systemImage: "heart")
                    .monospacedDigit()
            } else {
                Text("No heart rate readings")
                    .foregroundStyle(.secondary)
            }
            if let marker = row.uncertainty {
                UncertaintyLabel(marker: marker, explanation: row.uncertaintyExplanation, showsExplanation: true)
            }
        }
        .padding(.vertical, 2)
    }
}
