/**
 * PoolPilot Alerts Service
 * DEPLOY VERSION: 2025-01-ALERTS-VALID-ONLY
 */

import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";
import fs from "fs";
import path from "path";

// --------------------------------------------------
// 🔐 GOOGLE AUTH (Render-compatible)
// --------------------------------------------------
if (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) {
  const credsPath = "/tmp/gcp-creds.json";
  fs.writeFileSync(
    credsPath,
    process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
  );
  process.env.GOOGLE_APPLICATION_CREDENTIALS = credsPath;
}

// --------------------------------------------------
// 🔧 APP SETUP
// --------------------------------------------------
const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

// --------------------------------------------------
// 🌎 ENV VARS
// --------------------------------------------------
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

// --------------------------------------------------
// 📦 CLIENTS
// --------------------------------------------------
const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
const twilioClient = twilio(TWILIO_SID, TWILIO_AUTH);
const smsEnabled = SMS_ENABLED === "true";

// --------------------------------------------------
// 🧠 HELPERS
// --------------------------------------------------
function buildMessage(alert) {
  return `🚨 Pool Alert
${alert.system_name}
${alert.alert_type}

${alert.alert_summary}

Reply ACK if received.`;
}

// --------------------------------------------------
// ❤️ HEALTH CHECK
// --------------------------------------------------
app.get("/health", (_, res) => {
  res.json({ ok: true });
});

// --------------------------------------------------
// 📡 NOTIFY ENDPOINT
// --------------------------------------------------
app.post("/notify", async (req, res) => {
  try {
    const { token } = req.query;

    if (token !== NOTIFY_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    // --------------------------------------------------
    // 🎯 SELECT ONLY VALID, ANALYZED, UNSENT ALERTS
    // --------------------------------------------------
    const query = `
      SELECT
        snapshot_ts,
        system_id,
        system_name,
        alert_type,
        alert_summary,
        alert_phone,
        alert_email,
        classification
      FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
        AND classification = 'valid'
      ORDER BY snapshot_ts ASC
    `;

    const [rows] = await bq.query({ query });

    if (rows.length === 0) {
      return res.json({
        alerts_sent: 0,
        alerts_skipped: 0,
        dry_run: !smsEnabled
      });
    }

    let sent = 0;
    let skipped = 0;

    // --------------------------------------------------
    // 📤 SEND ALERTS
    // --------------------------------------------------
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

    // --------------------------------------------------
    // 🧾 MARK AS NOTIFIED (COMPOSITE KEY)
    // --------------------------------------------------
    const updateQuery = `
      UPDATE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      SET notified_at = CURRENT_TIMESTAMP()
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
        AND classification = 'valid'
    `;

    await bq.query({ query: updateQuery });

    res.json({
      alerts_sent: sent,
      alerts_skipped: skipped,
      dry_run: !smsEnabled
    });
  } catch (err) {
    console.error("❌ notify error:", err);
    res.status(500).json({
      error: "internal_error",
      message: err.message
    });
  }
});

// --------------------------------------------------
// 🚀 START SERVER
// --------------------------------------------------
app.listen(PORT, () => {
  console.log("🚀 DEPLOY VERSION: 2025-01-ALERTS-VALID-ONLY");
  console.log(`🚀 PoolPilot Alerts Service running on port ${PORT}`);
});
