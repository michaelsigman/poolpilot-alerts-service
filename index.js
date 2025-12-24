/**
 * PoolPilot Alerts Service
 * DEPLOY VERSION: 2025-01-ALERTS-VALID-ONLY-SMS-OVERRIDE-CONTACTS-AGENCY
 */

import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";
import fs from "fs";

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
  SMS_OVERRIDE_TO, // testing mode signal
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
function buildMessage(alert, isTestMode) {
  let msg = `PoolPilot Alert
${alert.system_name}
${alert.alert_type}

${alert.alert_summary}`;

  // 👇 ONLY include agency + contact info in TEST MODE
  if (isTestMode) {
    msg += `

--- Agency ---
${alert.agency_name || "Unknown Agency"}

--- Manager Contact ---
Phone: ${alert.alert_phone || "N/A"}
Email: ${alert.alert_email || "N/A"}`;
  }

  msg += `

Reply STOP to opt out.`;

  return msg;
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

    const isTestMode = Boolean(SMS_OVERRIDE_TO);

    // --------------------------------------------------
    // 🎯 SELECT ONLY VALID, ANALYZED, UNSENT ALERTS
    // --------------------------------------------------
    const selectQuery = `
      SELECT
        snapshot_ts,
        system_id,
        system_name,
        alert_type,
        alert_summary,
        alert_phone,
        alert_email,
        agency_name,
        classification
      FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
        AND classification = 'valid'
      ORDER BY snapshot_ts ASC
    `;

    const [rows] = await bq.query({ query: selectQuery });

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
      const toNumber = SMS_OVERRIDE_TO || alert.alert_phone;

      if (!toNumber) {
        skipped++;
        continue;
      }

      const body = buildMessage(alert, isTestMode);

      if (smsEnabled) {
        console.log("📲 Sending SMS", {
          to: toNumber,
          system: alert.system_name,
          agency: alert.agency_name || "unknown",
          testMode: isTestMode
        });

        await twilioClient.messages.create({
          from: TWILIO_FROM,
          to: toNumber,
          body
        });
      }

      sent++;
    }

    // --------------------------------------------------
    // 🧾 MARK AS NOTIFIED
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
  console.log(
    "🚀 DEPLOY VERSION: 2025-01-ALERTS-VALID-ONLY-SMS-OVERRIDE-CONTACTS-AGENCY"
  );
  console.log(`🚀 PoolPilot Alerts Service running on port ${PORT}`);
});
