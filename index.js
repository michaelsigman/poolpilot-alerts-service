import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";

console.log("🚀 DEPLOY VERSION: FINAL-NO-TIME-WINDOW");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

// ─────────────────────────────────────────────
// ENV
// ─────────────────────────────────────────────
const {
  BQ_PROJECT_ID,
  BQ_DATASET,
  BQ_TABLE,
  TWILIO_SID,
  TWILIO_AUTH,
  TWILIO_FROM,
  SMS_ENABLED,
  NOTIFY_TOKEN
} = process.env;

// ─────────────────────────────────────────────
// CLIENTS
// ─────────────────────────────────────────────
const bq = new BigQuery({
  projectId: BQ_PROJECT_ID
});

const twilioClient = twilio(TWILIO_SID, TWILIO_AUTH);
const smsEnabled = SMS_ENABLED === "true";

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function buildMessage(alert) {
  return `🚨 PoolPilot Alert

Property: ${alert.system_name}
Alert: ${alert.alert_type}

${alert.alert_summary}

Detected: ${alert.snapshot_pst ?? alert.snapshot_ts}

Reply or forward to schedule service.`;
}

// ─────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────
app.get("/health", (_, res) => {
  res.json({ ok: true });
});

app.post("/notify", async (req, res) => {
  try {
    const { token } = req.query;

    if (token !== NOTIFY_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    // ─────────────────────────────────────────
    // CORE QUERY (NO TIME WINDOW)
    // ─────────────────────────────────────────
    const query = `
      SELECT *
      FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
      ORDER BY snapshot_ts ASC
    `;

    const [rows] = await bq.query({ query });

    if (rows.length === 0) {
      return res.json({ alerts_sent: 0 });
    }

    let sent = 0;
    let skipped = 0;

    for (const alert of rows) {
      if (!alert.alert_phone && !alert.alert_email) {
        skipped++;
        continue;
      }

      const body = buildMessage(alert);

      if (smsEnabled && alert.alert_phone) {
        await twilioClient.messages.create({
          from: TWILIO_FROM,
          to: alert.alert_phone,
          body
        });
      }

      sent++;
    }

    // ─────────────────────────────────────────
    // MARK AS NOTIFIED
    // ─────────────────────────────────────────
    const markNotifiedQuery = `
      UPDATE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      SET notified_at = CURRENT_TIMESTAMP()
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
    `;

    await bq.query(markNotifiedQuery);

    res.json({
      alerts_sent: sent,
      alerts_skipped: skipped,
      dry_run: !smsEnabled
    });

  } catch (err) {
    console.error("❌ Notify error:", err);
    res.status(500).json({
      error: "internal_error",
      message: err.message
    });
  }
});

// ─────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`🚀 PoolPilot Alerts Service running on port ${PORT}`);
});
