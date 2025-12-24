import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";

const app = express();
app.use(express.json());

// 🔑 SIMPLE AUTH (INTENTIONAL FOR NOW)
const NOTIFY_TOKEN = "supersecretlongtoken";

// 🌍 REQUIRED BY RENDER
const PORT = process.env.PORT || 3000;

// ---- ENV ----
const {
  BQ_PROJECT_ID,
  BQ_DATASET,
  BQ_TABLE,
  TWILIO_SID,
  TWILIO_AUTH,
  TWILIO_FROM,
  SMS_ENABLED,
  GOOGLE_APPLICATION_CREDENTIALS_JSON
} = process.env;

// ---- VALIDATION ----
if (!GOOGLE_APPLICATION_CREDENTIALS_JSON) {
  console.error("❌ Missing GOOGLE_APPLICATION_CREDENTIALS_JSON");
  process.exit(1);
}

// ---- GOOGLE CREDS ----
let credentials;
try {
  credentials = JSON.parse(GOOGLE_APPLICATION_CREDENTIALS_JSON);
} catch (err) {
  console.error("❌ Failed to parse GOOGLE_APPLICATION_CREDENTIALS_JSON", err);
  process.exit(1);
}

// ---- CLIENTS ----
const bq = new BigQuery({
  projectId: BQ_PROJECT_ID,
  credentials
});

const twilioClient = twilio(TWILIO_SID, TWILIO_AUTH);

// ---- FLAGS ----
const smsEnabled = SMS_ENABLED === "true";

// ---- HELPERS ----
function buildMessage(alert) {
  return `🚨 Pool Alert
${alert.system_name}
${alert.alert_type}

A heater issue was detected and may require attention.`;
}

// ---- ROUTES ----
app.get("/health", (_, res) => {
  res.json({ ok: true });
});

app.post("/notify", async (req, res) => {
  try {
    const { token, minutes = 30 } = req.query;

    if (token !== NOTIFY_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    console.log(`🔔 /notify triggered (minutes=${minutes})`);

    const query = `
      SELECT *
      FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      WHERE notified_at IS NULL
        AND snapshot_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutes MINUTE)
      ORDER BY snapshot_ts ASC
    `;

    const [rows] = await bq.query({
      query,
      params: { minutes: Number(minutes) }
    });

    if (!rows || rows.length === 0) {
      console.log("✅ No new alerts");
      return res.json({ alerts: 0 });
    }

    let sent = 0;

    for (const alert of rows) {
      const body = buildMessage(alert);

      if (smsEnabled) {
        await twilioClient.messages.create({
          from: TWILIO_FROM,
          to: alert.sms_to,
          body
        });
      }

      sent++;
    }

    // 🧠 DEDUPE: mark snapshots as notified
    const ids = rows.map(r => `'${r.alert_id}'`).join(",");

    if (ids.length > 0) {
      await bq.query(`
        UPDATE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
        SET notified_at = CURRENT_TIMESTAMP()
        WHERE alert_id IN (${ids})
      `);
    }

    console.log(`📤 Alerts processed: ${sent}`);

    res.json({
      alerts_sent: sent,
      sms_sent: smsEnabled ? sent : 0,
      dry_run: !smsEnabled
    });

  } catch (err) {
    console.error("❌ /notify error:", err);
    res.status(500).json({
      error: "internal_error",
      message: err.message
    });
  }
});

// ---- START SERVER ----
app.listen(PORT, () => {
  console.log(`🚀 PoolPilot Alerts listening on port ${PORT}`);
});
