console.log("🚀 DEPLOY VERSION: 2025-01-ALERTS-COMPOSITE-KEY");

import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";
import fs from "fs";

const app = express();
app.use(express.json());

/* =====================================================
   CONFIG
   ===================================================== */

const PORT = process.env.PORT || 3000;

const {
  BQ_PROJECT_ID,
  BQ_DATASET,
  BQ_TABLE,
  TWILIO_SID,
  TWILIO_AUTH,
  TWILIO_FROM,
  SMS_ENABLED,
  NOTIFY_TOKEN,
  GOOGLE_APPLICATION_CREDENTIALS_JSON
} = process.env;

const smsEnabled = SMS_ENABLED === "true";

/* =====================================================
   GOOGLE AUTH (Render-safe)
   ===================================================== */

if (!GOOGLE_APPLICATION_CREDENTIALS_JSON) {
  throw new Error("Missing GOOGLE_APPLICATION_CREDENTIALS_JSON");
}

const credentialsPath = "/tmp/gcp-key.json";

if (!fs.existsSync(credentialsPath)) {
  fs.writeFileSync(credentialsPath, GOOGLE_APPLICATION_CREDENTIALS_JSON);
}

process.env.GOOGLE_APPLICATION_CREDENTIALS = credentialsPath;

/* =====================================================
   CLIENTS
   ===================================================== */

const bigquery = new BigQuery({
  projectId: BQ_PROJECT_ID
});

const twilioClient = twilio(TWILIO_SID, TWILIO_AUTH);

/* =====================================================
   HELPERS
   ===================================================== */

function buildMessage(alert) {
  return `🚨 Pool Alert
${alert.system_name}
${alert.alert_type}

${alert.alert_summary}

Agency: ${alert.agency_name}
📞 ${alert.alert_phone}
📧 ${alert.alert_email}`;
}

/* =====================================================
   ROUTES
   ===================================================== */

app.get("/health", (_, res) => {
  res.json({ ok: true });
});

app.post("/notify", async (req, res) => {
  try {
    const { token, minutes = 30 } = req.query;

    if (token !== NOTIFY_TOKEN) {
      return res.status(401).json({ error: "unauthorized" });
    }

    console.log("🔔 Notify run started");

    /* ---------------------------------------------
       Select unsent, analyzed alerts
       --------------------------------------------- */
    const selectQuery = `
      SELECT
        system_id,
        snapshot_ts,
        system_name,
        alert_type,
        alert_summary,
        agency_name,
        alert_phone,
        alert_email
      FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
      WHERE notified_at IS NULL
        AND alert_summary IS NOT NULL
        AND snapshot_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutes MINUTE)
      ORDER BY snapshot_ts ASC
    `;

    const [rows] = await bigquery.query({
      query: selectQuery,
      params: { minutes: Number(minutes) }
    });

    if (rows.length === 0) {
      console.log("✅ No alerts ready to send");
      return res.json({ alerts_sent: 0 });
    }

    let sent = 0;
    let skipped = 0;
    const keys = [];

    /* ---------------------------------------------
       Send alerts
       --------------------------------------------- */
    for (const alert of rows) {

      // Skip alerts with no delivery route
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
      keys.push({
        system_id: alert.system_id,
        snapshot_ts: alert.snapshot_ts,
        alert_type: alert.alert_type
      });
    }

    /* ---------------------------------------------
       Mark alerts as notified (composite key)
       --------------------------------------------- */
    if (keys.length > 0) {
      const conditions = keys.map(k =>
        `(system_id = '${k.system_id}'
          AND snapshot_ts = TIMESTAMP('${k.snapshot_ts}')
          AND alert_type = '${k.alert_type}')`
      ).join(" OR ");

      const updateQuery = `
        UPDATE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
        SET notified_at = CURRENT_TIMESTAMP()
        WHERE ${conditions}
      `;

      await bigquery.query(updateQuery);
    }

    console.log(`📤 Alerts sent: ${sent}, skipped: ${skipped}`);

    res.json({
      alerts_sent: sent,
      alerts_skipped: skipped,
      dry_run: !smsEnabled
    });

  } catch (err) {
    console.error("❌ Notify error", err);
    res.status(500).json({
      error: "internal_error",
      message: err.message
    });
  }
});

/* =====================================================
   START SERVER
   ===================================================== */

app.listen(PORT, () => {
  console.log(`🚀 PoolPilot Alerts Service running on port ${PORT}`);
});
