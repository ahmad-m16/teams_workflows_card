require("dotenv").config();
const { Client } = require("pg");
const axios = require("axios");
const cron = require("node-cron");

const QUERY = `
  select wp.id,
         p."name" as project_name,
         concat(u.firstname,' ',u.lastname) as assigned_to,
         wp.subject,
         wp.created_at::date as created_on,
         co.value as support_type
  from work_packages wp
  JOIN custom_values cv ON cv.customized_id = wp.id AND cv.customized_type = 'WorkPackage' 
  JOIN custom_fields cf ON cf.id = cv.custom_field_id
  join custom_options co on cv.value::int = co.id 
  left join projects p on p.id = wp.project_id
  left join users u on wp.assigned_to_id = u.id
  where type_id in ('13','14') and project_id = '3' and status_id != '12' and cf.id = 16
`;

async function fetchDataAndSendToTeams(queryText) {
  const client = new Client({
    host: process.env.PG_HOST,
    port: Number(process.env.PG_PORT || 5432),
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    // ssl: { rejectUnauthorized: false }, // uncomment if your DB requires SSL without proper CA
  });

  try {
    await client.connect();

    const { rows } = await client.query(queryText);
    if (!rows || rows.length === 0) {
      console.log("No rows found. Nothing to send.");
      return;
    }

    const webhookUrl = process.env.TEAMS_WEBHOOK_URL;
    if (!webhookUrl) throw new Error("TEAMS_WEBHOOK_URL missing");

    for (const row of rows) {
      const {
        id,
        project_name,
        assigned_to,
        subject,
        created_on,
        support_type,
      } = row;

      const card = {
        type: "message",
        summary: "Tickets Pending for Customer Support",
        attachments: [
          {
            contentType: "application/vnd.microsoft.card.adaptive",
            content: {
              $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
              type: "AdaptiveCard",
              version: "1.5",
              body: [
                {
                  type: "TextBlock",
                  text: "Tickets Pending for Customer Support",
                  size: "Large",
                  weight: "Bolder",
                  wrap: true,
                },
                {
                  type: "FactSet",
                  facts: [
                    { title: "Ticket No:", value: String(id) },
                    { title: "Project:", value: String(project_name ?? "") },
                    { title: "Assignee:", value: String(assigned_to ?? "") },
                    { title: "Subject", value: String(subject ?? "") },
                    { title: "Created Date:", value: String(created_on ?? "") },
                    {
                      title: "Support Type:",
                      value: String(support_type ?? ""),
                    },
                  ],
                },
              ],
            },
          },
        ],
      };

      try {
        await axios.post(webhookUrl, card, { timeout: 15000 });
        console.log(`Sent ticket ${id} to Teams`);
        await new Promise((r) => setTimeout(r, 200));
      } catch (postErr) {
        console.error(
          `Failed posting ticket ${id}:`,
          postErr?.response?.data || postErr.message
        );
      }
    }
  } catch (e) {
    console.error("Error in fetchDataAndSendToTeams:", e.message);
  } finally {
    await client.end().catch(() => {});
  }
}

cron.schedule(
  "0 10,15 * * *",
  async () => {
    console.log(`[${new Date().toISOString()}] Running scheduled job...`);
    await fetchDataAndSendToTeams(QUERY);
  },
  {
    timezone: "Asia/Kolkata",
  }
);
