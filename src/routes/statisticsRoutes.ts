import { Router } from "express";
import { DataSource } from "typeorm";
import { StatisticsService } from "../services/statisticsService";
import { StatisticsController } from "../controllers/statisticsController";

export const StatisticsRoutes = (ds: DataSource): Router => {
  const r = Router();
  const svc = new StatisticsService(ds);
  const ctrl = new StatisticsController(svc);

  // Route: GET /statistics/unidade/:id/json
  r.get("/unidade/:id/json", ctrl.unidadeJson);

  // Route: GET /statistics/unidade/:id/pdf
  r.get("/unidade/:id/pdf", ctrl.unidadePdf);

  // Route: GET /statistics/hospital/:id/json
  r.get("/hospital/:id/json", ctrl.hospitalJson);

  // Route: GET /statistics/hospital/:id/pdf
  r.get("/hospital/:id/pdf", ctrl.hospitalPdf);

  return r;
};
