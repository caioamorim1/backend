import { Router } from "express";
import { DataSource } from "typeorm";
import { runSessionExpiryForDate } from "../jobs/sessionExpiry";

export const JobsRoutes = (dataSource: DataSource) => {
  const router = Router();

  // POST /jobs/session-expiry { date: 'YYYY-MM-DD' }
  router.post("/session-expiry", async (req, res) => {
    const { date } = req.body || {};
    if (!date || typeof date !== "string") {
      return res
        .status(400)
        .json({ error: "body must include 'date' as YYYY-MM-DD" });
    }

    try {
      await runSessionExpiryForDate(dataSource, date);
      return res.status(200).json({ ok: true, date });
    } catch (e: any) {
      console.error("Failed to run session expiry:", e);
      return res
        .status(500)
        .json({ ok: false, error: e?.message || String(e) });
    }
  });

  return router;
};
