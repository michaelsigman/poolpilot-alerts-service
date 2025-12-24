import express from "express";
import { BigQuery } from "@google-cloud/bigquery";
import twilio from "twilio";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

// ---- ENV ----
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

// ---- CLIENTS ----
const bq = new BigQuery({ projectId: BQ_PROJECT_ID });
const twilioClient = twilio(TWILIO_SID, TWILIO_AUTH);

// ---- HELPERS ----
const smsEnabled = SMS_ENABLED === "true";

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
  const { token, minutes = 30 } = req.query;

  if (token !== NOTIFY_TOKEN) {
    return res.status(401).json({ error: "unauthorized" });
  }

  const query = `
    SELECT *
    FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
    WHERE alert_sent_ts IS NULL
      AND snapshot_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutes MINUTE)
    ORDER BY snapshot_ts ASC
  `;

  const [rows] = await bq.query({
    query,
    params: { minutes: Number(minutes) }
  });

  if (rows.length === 0) {
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

  // Mark alerts as sent
  const ids = rows.map(r => `'${r.alert_id}'`).join(",");
  await bq.query(`
    UPDATE \`${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`
    SET alert_sent_ts = CURRENT_TIMESTAMP()
    WHERE alert_id IN (${ids})
  `);

  res.json({
    alerts_sent: sent,
    dry_run: !smsEnabled
  });
});

// ---- START ----
app.listen(PORT, () => {
  console.log(`🚀 PoolPilot Alerts running on ${PORT}`);
});
