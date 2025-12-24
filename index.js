import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";

console.log("🚀 DEPLOY VERSION: FINAL-BQ-CREDS-NO-WINDOW");

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
  GOOGLE_APPLICATION_CREDENTIALS_JSON,
  TWILIO_SID,
  TWILIO_AUTH,
  TWILIO_FROM,
  SMS_ENABLED,
  NOTIFY_TOKEN
} = process.env;

// ─────────────────────────────────────────────
// BIGQUERY CLIENT (RENDER SAFE)
// ─────────────────────────────────────────────
let credentials = null;

if (GOOGLE_APPLICATION_CREDENTIALS_JSON) {
  try {
    credentials = JSON.parse(GOOGLE_APPLICATION_CREDENTIALS_JSON);
  } catch (err) {
    console.error("❌ Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON");
    throw err;
  }
}

const bq = new BigQuery({
  projectId: BQ_PROJECT_ID,
  credentials
});

// ─────────────────────────────────────────────
// TWILIO CLIENT
// ─────────────────────────────────────────────
const twilioClient = twilio(TWILIO_SID, TWILIO_AUTH);
const smsEnabled = SMS_ENABLED === "true";

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function buildMessage(alert) {
  const detectedAt = alert.snapshot_pst ?? alert.snapshot_ts;

  return `🚨 PoolPilot Alert

Property: ${alert.system_name}
Alert Type: ${alert.alert_type}

${alert.alert_summary}

Detected: ${detectedAt}

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
    // SELECT UNSENT, READY ALERTS (NO TIME WINDOW)
    // ─────────────────────────────────────────
    const selectQuery = `
      SELECT *
      FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
      ORDER BY snapshot_ts ASC
    `;

    const [rows] = await bq.query({ query: selectQuery });

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

      const message = buildMessage(alert);

      if (smsEnabled && alert.alert_phone) {
        await twilioClient.messages.create({
          from: TWILIO_FROM,
          to: alert.alert_phone,
          body: message
        });
      }

      sent++;
    }

    // ─────────────────────────────────────────
    // MARK ALL SENT ALERTS
    // ─────────────────────────────────────────
    const updateQuery = `
      UPDATE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      SET notified_at = CURRENT_TIMESTAMP()
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
    `;

    await bq.query({ query: updateQuery });

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
